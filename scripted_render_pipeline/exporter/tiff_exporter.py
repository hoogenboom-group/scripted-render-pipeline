import pathlib

import tifffile

from . import exporter


class Tiff_Exporter(exporter.Downloader):
    """exporter for tiff files from render api

    location: path to where to store the tiff files
    host: render host
    owner: render owner
    project: render project
    auth: basic auth as returned by basic_auth.load_auth, default None
    downscaling: how many times to downscale the images, default 1, no scaling
    concurrency: how many threads to use to download from render,
        default exporter.DEFAULT_CONCURRENCY
    """
    def __init__(self, location, *args, **kwargs):
        self.location = pathlib.Path(location)
        self.location.mkdir(parents=True, exist_ok=True)
        super().__init__(*args, **kwargs)

    def save(self, stack, index_x, index_y, index_z, data):  # overwrite
        path = self.location.joinpath(stack, f"{index_z}")
        path.mkdir(parents=True, exist_ok=True)
        filename = path.joinpath(f"{index_x}_{index_y}.tiff")
        tifffile.imwrite(filename, data)
