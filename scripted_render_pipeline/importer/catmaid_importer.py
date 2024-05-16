import re

import renderapi
import skimage

from . import mipmapper, render_specs

FILENAME_REGEX = re.compile("([0-9]+)_([0-9]+)_0.png")
IMAGE_FILENAME_PADDING = 3


class Catmaid_Mipmapper(mipmapper.Mipmapper):
    def __init__(self, *args, **kwargs):  # override
        super().__init__(*args, **kwargs)
        self.stack_name = self.project_path.name

    def find_files(self):  # override
        for zpath in self.project_path.iterdir():
            try:
                zval = int(zpath.name)
            except ValueError:
                continue

            for filepath in zpath.iterdir():
                if not filepath.is_file():
                    continue

                match = FILENAME_REGEX.fullmatch(filepath.name)
                if match is None:
                    continue

                yval = int(match.group(1))
                xval = int(match.group(2))
                yield filepath, xval, yval, zval

    def create_mipmaps(self, args):  # override
        file_path, xval, yval, zval = args
        y_by_x_str = "x".join(
            [str(xy).zfill(IMAGE_FILENAME_PADDING) for xy in [yval, xval]]
        )
        output_dir = self.mipmap_path / f"{zval}" / y_by_x_str
        output_dir.mkdir(parents=True, exist_ok=self.clobber)

        image = skimage.io.imread(file_path)
        length, width = image.shape
        pyramid = self.make_pyramid(output_dir, image, "")
        del image

        layout = renderapi.tilespec.Layout(
            sectionId=self.stack_name,
            pixelsize=1,
            imageRow=yval,
            imageCol=xval,
        )
        spec = renderapi.tilespec.TileSpec(
            imagePyramid=pyramid,
            layout=layout,
            width=width,
            height=length,
            tforms=[],
        )
        axes = [
            render_specs.Axis(0, width, xval * width),
            render_specs.Axis(0, length, yval * length),
        ]
        # there shouldn't be overlap, so this just has to be unique
        time = f"{zval}_{yval}_{xval}"
        tile = render_specs.Tile(
            self.stack_name, zval, spec, time, axes, 0, 0xFF
        )
        return [tile]
