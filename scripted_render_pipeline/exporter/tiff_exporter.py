import pathlib

import tifffile

from . import exporter


class Tiff_Exporter(exporter.Downloader):
    def __init__(self, location, *args, **kwargs):
        self.location = pathlib.Path(location)
        self.location.mkdir(parents=True, exist_ok=True)
        super().__init__(*args, **kwargs)

    def save(self, stack, index_x, index_y, index_z, data):  # overwrite
        path = self.location.joinpath(stack, index_z)
        path.mkdir(parents=True, exist_ok=True)
        filename = path.joinpath(f"{index_x}_{index_y}.tiff")
        tifffile.imwrite(filename, data)
