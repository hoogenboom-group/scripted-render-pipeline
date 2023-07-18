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
IMAGE_FILENAME_PADDING = 3
TIFFILE_GLOB = (
    "[0-9]" * IMAGE_FILENAME_PADDING
    + "_"
    + "[0-9]" * IMAGE_FILENAME_PADDING
    + "_0.tiff"
)
_rx_number_part = rf"\d{{{IMAGE_FILENAME_PADDING}}}"
TIFFILE_X_BY_Y_RX = re.compile(
    rf"(?P<x>{_rx_number_part})_(?P<y>{_rx_number_part})_0"
)
STACK_BAD_CHARACTER_RX = re.compile(r"[^0-9a-zA-Z_]+")
STACK_BAD_CHARACTER_REPLACEMENT = "_"
POSITIONS_LINE_RX = re.compile(
    rf"(?P<file>{_rx_number_part}_{_rx_number_part}_0.tiff) at "
    r"(?P<x>\d+), (?P<y>\d+)"
)


class FASTEM_Mipmapper(Mipmapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.positions = None

    def find_positions(self) -> bool:
        """try to find the positions.txt file and parse it

        the positions.txt file will be used when creating mipmaps and given to
        render as transforms instead of putting all images side by side
        returns whether the file was found
        """
        path = self.project_path / POSITIONS_FILENAME
        if not path.exists():
            path = self.project_path / CORRECTIONS_DIR / POSITIONS_FILENAME
            if not path.exists():
                self.positions = None
                return False

        self.positions = {}
        with path.open() as fp:
            fp.readline()
            for line in fp.readlines():
                match = POSITIONS_LINE_RX.match(line)
                if match is None:
                    self.positions = None
                    raise RuntimeError(
                        f"found positions.txt file at {path.absolute()} could "
                        "not be parsed"
                    )

                filename = match.group("file")
                coords = match.group("x"), match.group("y")
                self.positions[filename] = [int(coord) for coord in coords]

        return True

    def read_tiff(self, output_dir, file_path):
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
            if self.project_path.name == CORRECTIONS_DIR:
                # hacky way to get `DateTime` of corrected tiffs
                # from the corresponding raw tiff file
                file_path_to_raw = file_path.parents[1] / file_path.name
                raw_tiff = tifffile.TiffFile(file_path_to_raw)
                tags = raw_tiff.pages[0].tags
            timestr = tags["DateTime"].value
            time = datetime.datetime.fromisoformat(timestr)

        return pyramid, percentile, width, length, time

    def create_mipmaps(self, args):  # override
        file_path, project_name, section_name, zvalue, metadata = args
        match = TIFFILE_X_BY_Y_RX.fullmatch(file_path.stem)
        x_by_y = int(match.group("x")), int(match.group("y"))
        x_by_y_str = "x".join(
            [str(xy).zfill(IMAGE_FILENAME_PADDING) for xy in x_by_y]
        )
        output_dir = self.mipmap_path / x_by_y_str
        output_dir.mkdir(parents=True, exist_ok=self.clobber)
        pyramid, percentile, width, length, time = self.read_tiff(
            output_dir, file_path
        )
        pixel_size = metadata["pixel_size"] / 1000  # convert nm to um
        layout = renderapi.tilespec.Layout(
            sectionId=section_name,
            scopeId=SCOPE_ID,
            pixelsize=float(pixel_size),
            imageRow=x_by_y[1],
            imageCol=x_by_y[0]
        )
        spec = renderapi.tilespec.TileSpec(
            imagePyramid=pyramid,
            layout=layout,
            width=width,
            height=length,
        )
        pixels = width, length
        bbox = np.array([[0, 0], [0, pixels[1]], [pixels[0], 0], [*pixels]])
        mins = [min(*values) for values in zip(*bbox)]
        maxs = [max(*values) for values in zip(*bbox)]
        if self.positions is None:
            # x and y are flipped?
            rev = reversed(x_by_y)
            # assumes no overlap
            coordinates = [xy * px for xy, px in zip(rev, pixels)]
        else:
            # use saved coordinates from positions.txt
            try:
                coordinates = self.positions[file_path.name]
            except KeyError as exc:
                raise RuntimeError(
                    f"file at {file_path} was not found in positions.txt"
                ) from exc

        axes = [Axis(*item) for item in zip(mins, maxs, coordinates)]
        stack_name = STACK_BAD_CHARACTER_RX.sub(
            STACK_BAD_CHARACTER_REPLACEMENT, section_name
        )
        return [Tile(stack_name, zvalue, spec, time, axes, *percentile)]

    def find_files(self):  # override
        metadata_path = self.project_path / METADATA_FILENAME
        with metadata_path.open() as fp:
            metadata = yaml.safe_load(fp)

        logging.info(
            f"reading data from {self.project_path} using {self.parallel} "
            f"threads"
        )
        zvalue = 0
        if self.project_path.name == CORRECTIONS_DIR:
            *_, project_name, section_name, _ = self.project_path.parts
            section_name += "_" + CORRECTIONS_DIR
        else:
            *_, project_name, section_name = self.project_path.parts

        for file_path in self.project_path.glob(TIFFILE_GLOB):
            yield file_path, project_name, section_name, zvalue, metadata
