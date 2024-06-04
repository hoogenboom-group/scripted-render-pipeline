import abc
import concurrent.futures
import itertools
import logging

import numpy as np
import renderapi
import requests
import tifffile
import tqdm

SIZE = 1024  # download square chunks of SIZE by SIZE pixels
DEFAULT_CONCURRENCY = 8  # limiting factor is server ram


class Downloader(abc.ABC):
    """remote downloader for render api

    host, owner and project as used in render
    """

    def __init__(
        self,
        host,
        owner,
        project,
        auth=None,
        downscaling=1,
        concurrency=DEFAULT_CONCURRENCY,
    ):
        self.concurrency = concurrency
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=self.concurrency)
        session.mount(host, adapter)
        session.auth = auth

        self.render_params = dict(
            host=host, owner=owner, project=project, session=session
        )

        # for testing, scale down images 8 times
        self.scaledown = downscaling

        assert SIZE % self.scaledown == 0
        self.newsize = SIZE // self.scaledown
        self._extras_map = {}  # save these for debugging

    def download_project(self, stacks=None):
        """downloads stacks from the project

        stacks: list of stacks to download, defaults to all stacks
        """
        if stacks is None:
            # get list of stacks from render
            stacks = renderapi.render.get_stacks_by_owner_project(
                **self.render_params
            )

        for stack in stacks:
            self.download_stack(stack)

    @abc.abstractmethod
    def save(self, stack, index_x, index_y, index_z, data):
        """save the data"""

    def download_tile(
        self,
        stack,
        index_x,
        coord_x,
        index_y,
        coord_y,
        coord_z,
    ):
        result = renderapi.image.get_bb_image(
            stack=stack,
            x=coord_x,
            y=coord_y,
            z=coord_z,
            width=SIZE,
            height=SIZE,
            scale=1 / self.scaledown,
            **self.render_params,
        )
        if type(result) is renderapi.errors.RenderError:
            raise result

        logging.debug(f"got result array {result.shape}, dropping third axis")
        # take the blue pixel value of the grayscale image
        self.save(stack, index_x, index_y, int(coord_z), result[:, :, 1])

    def _setup_z(self, stack, z_values, *size):
        pass

    def download_stack(self, stack, z_values=None):
        if z_values is None:
            # get list of z_values from render
            z_values = renderapi.stack.get_z_values_for_stack(
                stack=stack, **self.render_params
            )

        if not z_values:
            raise ValueError(f"no z values to download from stack {stack}")

        # get bounds from render
        bounds = renderapi.stack.get_stack_bounds(
            stack=stack, **self.render_params
        )
        logging.info(f"zvalues {z_values}, bounds {bounds}")

        # calculate ranges
        bbox = bounds["minX"], bounds["minY"], bounds["maxX"], bounds["maxY"]
        bbox = map(int, bbox)
        min_x, min_y, max_x, max_y = bbox
        mins = min_x, min_y
        maxs = max_x, max_y
        totals = [max_ - min_ for min_, max_ in zip(mins, maxs)]
        # split remainder over both sides and round up
        extras = [total % SIZE // 2 + 1 for total in totals]
        self._extras_map[stack] = extras
        ranges = [
            range(min_ - extra, max_, SIZE)
            for min_, max_, extra in zip(mins, maxs, extras)
        ]

        futures = []
        self._setup_z(stack, z_values, len(ranges[1]), len(ranges[0]))
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency
        ) as executor:
            for coord_z in z_values:
                enumerated = [enumerate(range_) for range_ in ranges]
                for item in itertools.product(*enumerated):
                    (index_x, coord_x), (index_y, coord_y) = item
                    future = executor.submit(
                        self.download_tile,
                        stack,
                        index_x,
                        coord_x,
                        index_y,
                        coord_y,
                        coord_z,
                    )
                    futures.append(future)

            for future in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="rendering images",
                unit="images",
            ):
                try:
                    future.result()
                except Exception as exc:
                    raise exc


class Array_Downloader(Downloader):
    def __init__(self, *args, **kwargs):
        self.imgs = {}
        self._super = super()
        self._super.__init__(*args, **kwargs)

    def download_project(self, *args, **kwargs):  # overwrite
        self.imgs = {}
        self._super.download_project(*args, **kwargs)

    def get_stacks(self, stacks=None):
        self.download_project(stacks)
        imgs = self.imgs
        self.imgs = {}
        return imgs

    def download_stack(self, stack):  # overwrite
        self.imgs[stack] = {}
        self._super.download_stack(stack)

    def _setup_z(self, stack, z_values, *size):  # overwrite
        imgsize = [item * self.newsize for item in size]
        for z_value in z_values:
            self.imgs[stack][z_value] = np.empty(imgsize, dtype=np.uint8)

    def save(self, stack, index_x, index_y, index_z, data):  # overwrite
        coords = [index * self.newsize for index in (index_x, index_y)]
        slice_x, slice_y = [
            slice(coord, coord + self.newsize) for coord in coords
        ]
        self.imgs[stack][index_z][slice_y, slice_x] = data


def _draw_debug_marks(img, size, extras=None):
    # draw a solid line around the whole image
    img[0, :] = 0xFF
    img[-1, :] = 0xFF
    img[:, 0] = 0xFF
    img[:, -1] = 0xFF

    # draw tile borders with 3px dashes
    for x in range(size, img.shape[1], size):
        for i in range(3):
            img[i::6, x] = 0xFF
            img[i::6, x + 1] = 0xFF

    for y in range(size, img.shape[0], size):
        for i in range(3):
            img[y, i::6] = 0xFF
            img[y + 1, i::6] = 0xFF

    if extras is None:
        return

    # draw a little cross at the location of the min and max locations
    borders = [extra * size // SIZE for extra in extras]
    for border in borders:
        if border < 12:
            logging.debug(
                f"image padding too small to show min and max "
                f"locations: {borders}"
            )
            return

    coords = [
        (border, shape - border)
        for border, shape in zip(borders, reversed(img.shape))
    ]
    for x, y in itertools.product(*coords):
        img[y, x - 12 : x + 12] = 0xFF
        img[y - 12 : y + 12, x] = 0xFF


def _main():
    from ..basic_auth import load_auth

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    host = "https://sonic.tnw.tudelft.nl"
    owner = "rlane"
    project = "20230523_singleholder_Earthworm_03_partial_partial_test"
    test_scale = 8
    downloader = Array_Downloader(
        host,
        owner,
        project,
        load_auth(),
        test_scale,
    )
    stacks = downloader.get_stacks()
    key = next(iter(stacks.keys()))  # just write the first stack
    imgs = stacks[key]
    extras = downloader._extras_map[key]
    img = next(iter(imgs.values()))  # just write the first image in the stack
    _draw_debug_marks(img, downloader.newsize, extras)
    out = "o.tiff"
    logging.info(f"write {out} shape {img.shape}")
    tifffile.imwrite(out, img)


if __name__ == "__main__":
    _main()
