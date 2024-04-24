import logging
import pathlib

import cluster_tools
import webknossos

from . import exporter

DEFAULT_VOXEL_SIZE = 4, 4, 100


class Webknossos_Exporter(exporter.Downloader):
    """exporter from render to webknossos datasets

    downloads datasets from the http render api, then saves the dataset to disk
    in webknossos format

    location: path to save the webknossos dataset on disk
        note, when using an existing dataset layers can only be added
    host: render hostname
    owner: render owner
    project: render project
    downscaling: for testing the render project can be downscaled
        note, this affects voxel_size
        defaults to 1 (real size)
    concurrency: amount of threads to use to download concurrently
        defaults to exporter.DEFAULT_CONCURRENCY
    voxel_size: size of the voxels in the webknossos dataset
        defaults to DEFAULT_VOXEL_SIZE
    downsample: how many times to downsample the data
        defaults to 7 (max scale of 128 voxels)
    processes: how many parallel processes to use for downscaling
        defaults to 8
    """

    def __init__(
        self,
        location,
        *args,
        voxel_size=DEFAULT_VOXEL_SIZE,
        downsample=7,
        processes=8,
        **kwargs,
    ):
        self.location = pathlib.Path(location)
        self.voxel_size = [voxel * self.downscling for voxel in voxel_size]
        self.max_mag = webknossos.geometry.Mag(2**downsample)
        self.processes = processes
        self.location.mkdir(parents=True, exist_ok=True)
        name = self.location.name
        self.dataset = webknossos.dataset.Dataset(
            self.location, self.voxel_size, name, True
        )
        self.mags = {}
        self._super = super()
        self._super.__init__(*args, **kwargs)

    def _setup_z(self, stack, z_values, *size):  # overwrite
        first_z = z_values[0]
        z_size = z_values[-1] - first_z + 1
        layer = self.dataset.add_layer(stack, "color")
        layer.bounding_box = webknossos.geometry.BoundingBox(
            [0] * 2 + [first_z],
            [item * self.newsize for item in size] + [z_size],
        )
        mag = layer.add_mag(1)
        self.mags[stack] = mag

    def download_stack(self, stack):  # overwrite
        self._super.download_stack(stack)

        mag = self.mags[stack]
        mag.compress()
        if self.skip_downsample:
            return

        with cluster_tools.MultiprocessingExecutor(
            max_workers=self.processes
        ) as executor:
            layer = self.dataset.get_layer(stack)
            layer.downsample(
                mag.mag,
                self.max_mag,
                "bicubic",
                True,
                "anisotropic",
                executor=executor,
            )

    def save(self, stack, index_x, index_y, index_z, data):  # overwrite
        mag = self.mags[stack]
        coords = [coord * self.newsize for coord in (index_x, index_y)]
        offset = webknossos.geometry.Vec3Int(*coords, index_z)
        size = (self.newsize, self.newsize, 1)
        view = mag.get_view(absolute_offset=offset, size=size)
        logging.debug(
            f"writing data {data.shape} to layer {stack} at {offset}"
        )
        view.write(data.transpose().reshape(size), absolute_offset=offset)


def _main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    host = "https://sonic.tnw.tudelft.nl"
    owner = "rlane"
    project = "20230523_singleholder_Earthworm_03_partial_partial_test"
    path = "./wk_dataset"
    test_scale = 8
    concurrency = 8
    wk_exporter = Webknossos_Exporter(
        path,
        host,
        owner,
        project,
        downscaling=test_scale,
        concurrency=concurrency,
    )
    wk_exporter.download_project()
    logging.info(f"downloaded project, saved webknossos dataset to {path}")


if __name__ == "__main__":
    _main()