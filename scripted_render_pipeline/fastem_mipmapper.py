import datetime
import logging
import re

import numpy as np
import renderapi
import tifffile
import yaml

from .mipmapper import Mipmapper
from .render_specs import Axis, Tile

METADATA_FILENAME = "mega_field_meta_data.yaml"
SCOPE_ID = "FASTEM"

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


class FASTEM_Mipmapper(Mipmapper):
    def read_tiff(self, output_dir, file_path):
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
            timestr = tags["DateTime"].value
            time = datetime.datetime.fromisoformat(timestr)

        return pyramid, percentile, width, length, time

    def create_mipmaps(self, args):
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
            sectionId=f"{project_name}/{section_name}",
            scopeId=SCOPE_ID,
            pixelsize=float(pixel_size),
        )
        spec = renderapi.tilespec.TileSpec(
            imagePyramid=pyramid,
            layout=layout,
        )
        pixels = width, length
        bbox = np.array([[0, 0], [0, pixels[1]], [pixels[0], 0], [*pixels]])
        mins = [min(*values) for values in zip(*bbox)]
        maxs = [max(*values) for values in zip(*bbox)]
        # assumes no overlap
        um_position = [xy * px * pixel_size for xy, px in zip(x_by_y, pixels)]
        zipped = zip(mins, maxs, um_position)
        axes = [Axis(*item, pixel_size) for item in zipped]
        stack_name = STACK_BAD_CHARACTER_RX.sub(
            STACK_BAD_CHARACTER_REPLACEMENT, section_name
        )
        return [Tile(stack_name, zvalue, spec, time, axes, *percentile)]

    def find_files(self):
        metadata_path = self.project_path / METADATA_FILENAME
        with metadata_path.open() as fp:
            metadata = yaml.safe_load(fp)

        logging.info(
            f"reading data from {self.project_path} using {self.parallel} "
            f"threads"
        )
        zvalue = 0
        *_, project_name, section_name = self.project_path.parts
        for file_path in self.project_path.glob(TIFFILE_GLOB):
            yield file_path, project_name, section_name, zvalue, metadata
