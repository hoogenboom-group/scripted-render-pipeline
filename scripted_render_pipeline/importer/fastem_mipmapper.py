import datetime
import logging
import re

import numpy as np
import renderapi
import tifffile
import yaml

from .mipmapper import Mipmapper
from .render_specs import Axis, Tile

SCOPE_ID = "FASTEM"

METADATA_FILENAME = "mega_field_meta_data.yaml"
POSITIONS_FILENAME = "positions.txt"
CORRECTIONS_DIR = "corrected"
DEFAULT_STACK_NAME = "raw"
IMAGE_FILENAME_PADDING = 3
TIFFILE_GLOB = (
    "[0-9]" * IMAGE_FILENAME_PADDING
    + "_"
    + "[0-9]" * IMAGE_FILENAME_PADDING
    + "_0.tiff"
)
_rx_number_part = rf"\d{{{IMAGE_FILENAME_PADDING}}}"
TIFFILE_Y_BY_X_RX = re.compile(
    rf"(?P<y>{_rx_number_part})_(?P<x>{_rx_number_part})_0"
)
STACK_BAD_CHARACTER_RX = re.compile(r"[^0-9a-zA-Z_]+")
STACK_BAD_CHARACTER_REPLACEMENT = "_"
POSITIONS_LINE_RX = re.compile(
    rf"(?P<file>{_rx_number_part}_{_rx_number_part}_0.tiff) at "
    r"(?P<x>\d+), (?P<y>\d+)"
)


class FASTEM_Mipmapper(Mipmapper):
    """creates mipmaps from images and collects tile specs for the fastem

    project_path: path to project to make mipmaps for
    parallel: how many threads to use in parallel, optimises io usage
    clobber: wether to allow overwriting of existing mipmaps
    mipmap_path: where to save mipmaps, defaults to project_path/_mipmaps

    additional optional named arguments:
    project_paths: iterable of multiple project paths, indexed by zlevel
    use_positions: use the transforms from the positions.txt file
    """

    def __init__(
        self, *args, project_paths=None, use_positions=False, **kwargs
    ):
        try:
            project_path, *args = args
        except ValueError:
            try:
                project_path = kwargs.pop("project_path")
            except KeyError:
                if project_paths is None:
                    raise TypeError(
                        f"{self.__class__}() missing required argument: "
                        f"either 'project_path' or 'project_paths'"
                    ) from None
                elif project_paths:
                    try:
                        first_key = sorted(project_paths.keys())[0]
                        project_path = project_paths[first_key]
                    except AttributeError:
                        project_path = project_paths[0]
                else:
                    raise TypeError(
                        f"{self.__class__}() requires an iterable with at "
                        f"least one path for 'project_paths'"
                    ) from None

        if project_paths is None:
            project_paths = {0: project_path}

        super().__init__(project_path, *args, **kwargs)
        self.project_paths = project_paths
        self.use_positions = use_positions

    def find_positions(self, project_path):
        """try to find the positions.txt file and parse it

        the positions.txt file will be used when creating mipmaps and given to
        render as transforms instead of putting all images side by side
        project_path: path of the project where to find the positions.txt
        returns the positions as dict or None if not found
        """
        path = project_path / POSITIONS_FILENAME
        if not path.exists():
            path = path / CORRECTIONS_DIR / POSITIONS_FILENAME
            if not path.exists():
                return None

        positions = {}
        with path.open() as fp:
            fp.readline()
            for line in fp.readlines():
                match = POSITIONS_LINE_RX.match(line)
                if match is None:
                    positions = None
                    raise RuntimeError(
                        f"found positions.txt file at {path.absolute()} could "
                        "not be parsed"
                    )

                filename = match.group("file")
                coords = match.group("x"), match.group("y")
                positions[filename] = [int(coord) for coord in coords]

        return positions

    def read_tiff(self, output_dir, file_path, is_corrected):
        """read one tiff and generate mipmaps

        output_dir: location to put mipmaps
        file_path: file path to the tiff file to read
        """
        with tifffile.TiffFile(file_path) as tiff:
            if not tiff.pages:
                raise RuntimeError(f"found empty tifffile: {file_path}")

            description = ""
            image = tiff.pages[0].asarray()
            pyramid = self.make_pyramid(output_dir, image, description)
            intensity_clip = 1, 99
            percentile = np.percentile(image, intensity_clip)
            tags = tiff.pages[0].tags
            width, length = tags["ImageWidth"].value, tags["ImageLength"].value

            # corrected tiffs don't include `DateTime` tag for some reason
            if is_corrected:
                # hacky way to get `DateTime` of corrected tiffs
                # from the corresponding raw tiff file
                file_path_to_raw = file_path.parents[1] / file_path.name
                with tifffile.TiffFile(file_path_to_raw) as raw_tiff:
                    raw_tags = raw_tiff.pages[0].tags
                    timestr = raw_tags["DateTime"].value
            else:
                timestr = tags["DateTime"].value

            time = datetime.datetime.fromisoformat(timestr)

        return pyramid, percentile, width, length, time

    def create_mipmaps(self, args):  # override
        (
            file_path,
            project_name,
            section_name,
            zvalue,
            metadata,
            positions,
            is_corrected,
        ) = args
        match = TIFFILE_Y_BY_X_RX.fullmatch(file_path.stem)
        row, col = int(match.group("y")), int(match.group("x"))
        y_by_x_str = "x".join(
            [str(xy).zfill(IMAGE_FILENAME_PADDING) for xy in [row, col]]
        )
        output_dir = self.mipmap_path / f"{zvalue}" / y_by_x_str
        output_dir.mkdir(parents=True, exist_ok=self.clobber)
        pyramid, percentile, width, length, time = self.read_tiff(
            output_dir, file_path, is_corrected
        )
        pixel_size = metadata["pixel_size"] / 1000  # convert nm to um
        layout = renderapi.tilespec.Layout(
            sectionId=section_name,
            scopeId=SCOPE_ID,
            pixelsize=float(pixel_size),
            imageRow=row,
            imageCol=col,
        )
        spec = renderapi.tilespec.TileSpec(
            imagePyramid=pyramid,
            layout=layout,
            width=width,
            height=length,
            tforms=[],
        )
        pixels = width, length
        mins = [min(0, value) for value in pixels]
        maxs = [max(0, value) for value in pixels]
        if positions is None:
            # assumes no overlap
            coordinates = [xy * px for xy, px in zip([col, row], pixels)]
        else:
            # use saved coordinates from positions.txt
            try:
                coordinates = positions[file_path.name]
            except KeyError as exc:
                raise RuntimeError(
                    f"file at {file_path} was not found in positions.txt"
                ) from exc

        axes = [Axis(*item) for item in zip(mins, maxs, coordinates)]
        if is_corrected:
            stack_name = CORRECTIONS_DIR
        else:
            stack_name = DEFAULT_STACK_NAME

        return [Tile(stack_name, zvalue, spec, time, axes, *percentile)]

    def find_files(self):  # override
        logging.info(
            f"reading data from {len(self.project_paths)} section(s) using "
            f"{self.parallel} threads"
        )
        try:
            iterator = self.project_paths.items()
        except AttributeError:
            iterator = enumerate(self.project_paths)

        for zvalue, path in iterator:
            for items in self.find_files_in_section(zvalue, path):
                yield items

    def find_files_in_section(self, zvalue, path):
        metadata_path = path / METADATA_FILENAME
        with metadata_path.open() as fp:
            metadata = yaml.safe_load(fp)

        is_corrected = path.name == CORRECTIONS_DIR
        if is_corrected:
            *_, project_name, section_name, _ = path.parts
            section_name += "_" + CORRECTIONS_DIR
        else:
            *_, project_name, section_name = path.parts

        if self.use_positions:
            positions = self.find_positions(path)
        else:
            positions = None

        for file_path in path.glob(TIFFILE_GLOB):
            yield (
                file_path,
                project_name,
                section_name,
                zvalue,
                metadata,
                positions,
                is_corrected,
            )
