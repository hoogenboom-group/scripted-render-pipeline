import copy
import datetime
import logging
import re
import xml.etree.ElementTree

import numpy as np
import renderapi
import skimage.transform
import tifffile

from .mipmapper import Mipmapper
from .render_specs import Axis, Tile

# constants
SECTION_DIR_PADDING = 3  # amount of digits in a section directory
SECTION_DIR_GLOB = "S" + "[0-9]" * SECTION_DIR_PADDING
# amount of digits in each coordinate on an image file name
IMAGE_FILENAME_PADDING = 5
TIFFILE_GLOB = (
    "/tile-"
    + "[0-9]" * IMAGE_FILENAME_PADDING
    + "x"
    + "[0-9]" * IMAGE_FILENAME_PADDING
    + ".tif"
)
NOT_NUMBER_RX = re.compile("[^0-9]")

_rx_number_part = rf"\d{{{IMAGE_FILENAME_PADDING}}}"
TIFFILE_X_BY_Y_RX = re.compile(
    rf"tile-(?P<x>{_rx_number_part})x(?P<y>{_rx_number_part})"
)
# name of a directory mapped to the name of the stack for the EM data in it
DIR_BY_DATATYPE = {"CLEM-grid": "EM_lomag", "EM-grid": "EM_himag"}

# register the default namespace used in the OME image metadata xml, this is
# needed for etree to export xmls without the long namespace on every key
OME_NAMESPACE_URI = "http://www.openmicroscopy.org/Schemas/OME/2012-06"
NAMESPACE = {"": OME_NAMESPACE_URI}
xml.etree.ElementTree.register_namespace("", OME_NAMESPACE_URI)


class CLEM_Mipmapper(Mipmapper):
    def create_mipmaps(self, args):  # override
        file_path, section_name, zvalue, datatype_dir = args
        match = TIFFILE_X_BY_Y_RX.fullmatch(file_path.stem)
        x_by_y = int(match.group("x")), int(match.group("y"))
        tiles = []
        logging.debug(f"reading {file_path}")
        with tifffile.TiffFile(file_path) as tiff:
            if not tiff.pages:
                raise RuntimeError(f"found empty tifffile: {file_path}")

            # tiff files are saved in an approximation of the OME-TIFF format,
            # the metadata is saved as an OME-XML in the description of the
            # first tiff IFD
            metadata = tiff.pages[0].description
            try:
                root = xml.etree.ElementTree.fromstring(metadata)
            except xml.etree.ElementTree.ParseError:
                # In newly acquired datasets the first 7 lines are ImageJ stuff
                # Remove them and try again
                metadata = metadata.split("\n", 7)[-1]
                root = xml.etree.ElementTree.fromstring(metadata)

            image_elements = root.findall("Image", NAMESPACE)
            image_elements_by_name = {
                element.attrib["Name"]: element for element in image_elements
            }
            instrument = root.find("Instrument", NAMESPACE)
            detector_by_id = {}
            for detector in instrument.findall("Detector", NAMESPACE):
                _, detector_id = detector.attrib["ID"].split(":")
                detectorname = detector.attrib["Model"]
                detector_by_id[detector_id] = detectorname

            for page in tiff.pages:
                tile = self.create_mipmap_from_page(
                    page,
                    x_by_y,
                    root,
                    image_elements_by_name,
                    detector_by_id,
                    datatype_dir,
                    file_path,
                    section_name,
                    zvalue,
                )
                tiles.append(tile)

        return tiles

    def create_mipmap_from_page(
        self,
        page,
        x_by_y,
        root,
        image_elements_by_name,
        detector_by_id,
        datatype_dir,
        file_path,
        section_name,
        zvalue,
    ):
        """create mipmaps for a single page in the multipage tiffile

        page: tifffile.TiffPage to interpret
        x_by_y: x and y count as tuple
        root: metadata from this tifffile
        image_elements_by_name: dictionary of metadata for image elements
        detector_by_id: dictionary of detector names for ids
        datatype_dir: type of capture
        file_path: path of this tifffile
        section_name: name of this stack
        zvalue: z height of this section
        """
        tags = page.tags
        channel = tags["PageName"].value
        width, height = tags["ImageWidth"].value, tags["ImageLength"].value
        element = image_elements_by_name[channel]
        new_root = copy.copy(root)
        for other in image_elements_by_name.values():
            if other != element:
                new_root.remove(other)

        description = xml.etree.ElementTree.tostring(
            new_root,
            encoding="unicode",
            xml_declaration=False,
        )
        # tifffile.OmeXml.validate(description)
        image = page.asarray()
        pixels = element.find("Pixels", NAMESPACE)
        if channel == "Secondary electrons":
            name = DIR_BY_DATATYPE[datatype_dir]
            image = skimage.util.invert(image)  # invert the SEM image
            intensity_clip = 1, 99
        elif (
            channel.startswith("Filtered colour ")
            and datatype_dir == "CLEM-grid"
        ):
            pixel_channel = pixels.find("Channel", NAMESPACE)
            wavelength = pixel_channel.attrib["ExcitationWavelength"]
            name = f"exc_{wavelength}nm"
            intensity_clip = 30, 99
        else:
            raise RuntimeError(
                f"found unexpected channel '{channel}' in tifffile: "
                f"{file_path}"
            )

        x_by_y_str = "x".join(
            [str(xy).zfill(IMAGE_FILENAME_PADDING) for xy in x_by_y]
        )
        output_dir = self.mipmap_path / name / section_name / x_by_y_str
        output_dir.mkdir(parents=True, exist_ok=self.clobber)
        pyramid = self.make_pyramid(output_dir, image, description)
        percentile = np.percentile(image, intensity_clip)

        # find instrument metadata
        # NOTE: in the layout metadata scopeId becomes temca and cameraId
        # becomes camera. getting modelname dynamically doesn't work because
        # the EM-grid doesn't have it!
        # instrument = root.find("Instrument", NAMESPACE)
        # scope = instrument.find("Microscope", NAMESPACE)
        # modelname = scope.attrib["Model"]
        modelname = "SECOM"
        # this assumes each objective has an associated detector with that id,
        # the image only includes the objective id
        objective_settings = element.find("ObjectiveSettings", NAMESPACE)
        _, objective_id = objective_settings.attrib["ID"].split(":")
        try:
            detectorname = detector_by_id[objective_id]
        except KeyError as exc:
            raise RuntimeError(
                f"could not find associated detector with objective "
                f"{objective_id}"
            ) from exc

        timestr = element.find("AcquisitionDate", NAMESPACE).text
        time = datetime.datetime.fromisoformat(timestr)
        plane = pixels.find("Plane", NAMESPACE)

        tforms = []
        transform = element.find("Transform", NAMESPACE)
        if transform is not None:
            # load transform from spec (often a rotation)
            model = renderapi.transform.AffineModel()
            model.M00 = transform.attrib["A00"]
            model.M01 = transform.attrib["A01"]
            model.M10 = transform.attrib["A10"]
            model.M11 = transform.attrib["A11"]
            model.B0 = transform.attrib["A02"]
            model.B1 = transform.attrib["A12"]
            model.load_M()
            tforms.append(model)

        XY = "X", "Y"
        # size per pixel in micrometers
        size = [float(pixels.attrib["PhysicalSize" + xy]) for xy in XY]
        # scaling on y axis needed to align with an x scaled to 1
        x_size, y_size = size
        y_corrected = float(y_size / x_size)
        if y_corrected:
            tforms.append(renderapi.transform.AffineModel(M11=y_corrected))

        # invert y
        # tforms.append(renderapi.transform.AffineModel(M11=-1))

        # pixel count
        pixels = [int(pixels.attrib["Size" + xy]) for xy in XY]
        # stage position
        # NOTE: even though the OME spec specifies this parameter in um it is
        # erroneously saved in meters
        position = [plane.attrib["Position" + xy] for xy in XY]
        # NOTE: the y position needs to be inverted, the input data has origin
        # in the bottom left corner
        um_position = [  # convert to micrometers
            float(pos) * 1e6 * invert for pos, invert in zip(position, (1, -1))
        ]

        # calculate boundary box
        bbox = np.array([[0, 0], [0, pixels[1]], [pixels[0], 0], [*pixels]])
        for tform in tforms:
            bbox = tform.tform(bbox)

        mins = [min(*values) for values in zip(*bbox)]
        maxs = [max(*values) for values in zip(*bbox)]
        axes = [Axis(*item, x_size) for item in zip(mins, maxs, um_position)]

        # take the x pixel size only, transform is applied for scale difference
        layout = renderapi.tilespec.Layout(
            sectionId=f"{section_name}",
            scopeId=modelname,
            cameraId=detectorname,
            pixelsize=float(x_size),
        )
        layout.stageX, layout.stageY = [float(value) for value in um_position]
        layout.imageCol, layout.imageRow = x_by_y
        spec = renderapi.tilespec.TileSpec(
            imagePyramid=pyramid,
            layout=layout,
            width=width,
            height=height,
            tforms=tforms,
        )
        return Tile(name, zvalue, spec, time, axes, *percentile)

    def find_files(self):  # override
        section_paths = [*sorted(self.project_path.glob(SECTION_DIR_GLOB))]
        if not section_paths:
            raise RuntimeError(f"no files found at {self.project_path}")

        logging.info(
            f"reading {len(section_paths)} section"
            f"{'s' if len(section_paths) else ''} from {self.project_path} "
            f"using {self.parallel} threads"
        )
        first_z = None
        for section_path in section_paths:
            try:
                zvalue = int(NOT_NUMBER_RX.sub("", section_path.stem))
            except ValueError as exc:
                raise RuntimeError(
                    f"could not get z value from path {section_path}"
                ) from exc

            if first_z is None:
                first_z = zvalue
                zvalue = 0
            else:
                zvalue -= first_z

            section_name = section_path.name
            for datatype_dir in DIR_BY_DATATYPE.keys():
                files = [*section_path.glob(datatype_dir + TIFFILE_GLOB)]
                for file_path in files:
                    yield file_path, section_name, zvalue, datatype_dir
