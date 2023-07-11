import abc
import concurrent.futures
import logging
import pathlib
import typing

import numpy as np
import renderapi
import skimage.transform
import tifffile
from tqdm import tqdm

from .render_specs import Section, Stack

BASE_URL = ""  # "file://"


class Mipmapper(abc.ABC):
    """creates mipmaps from images and collects tile specs

    project_path: path to project to make mipmaps for
    parallel: how many threads to use in parallel, optimises io usage
    clobber: wether to allow overwriting of existing mipmaps
    mipmap_path: where to save mipmaps, defaults to project_path/_mipmaps
    """

    def __init__(
        self, project_path, parallel=1, clobber=False, mipmap_path=None
    ):
        self.remote = False
        self.project_path = project_path
        self.clobber = clobber
        self.parallel = parallel
        if mipmap_path is None:
            self.mipmap_path = project_path / "_mipmaps"
        else:
            self.mipmap_path = mipmap_path

    def set_remote_path(self, nas_share_path, server_storage_path_str):
        """set mipmapper to use remote paths

        will translate nas_share_path to server_storage_path_str
        """
        self.nas_share_path = nas_share_path
        self.server_storage_path_str = server_storage_path_str
        self.remote = True

    def to_server_path(self, path: pathlib.Path) -> str:
        """convert a local path to the location on the server

        path:
            an absolute path on this machine that is mapped to a server
            location
        returns a posix format server path as string
        """
        if not self.remote:
            return path.as_posix()

        total_parts = len(self.nas_share_path.parts)
        if path.parts[:total_parts] != self.nas_share_path.parts:
            raise ValueError(f"path {path} is not on the share")

        return self.server_storage_path_str + "/".join(
            path.parts[total_parts:]
        )

    def make_pyramid(
        self, output_dir: pathlib.Path, image: np.ndarray, description: str
    ) -> renderapi.image_pyramid.ImagePyramid:
        """create an image pyramid from image data and save it

        uses skimage.transform.pyramid_gaussian to make mipmap images

        output_dir: all images are written to output_dir as tiff
        image: image data as array
        description: will be added to the base level tiff image
        returns the render pyramid
        """
        leveldict = {}
        pyramid = skimage.transform.pyramid_gaussian(
            image, downscale=2, max_layer=8, preserve_range=True
        )
        for level, pyramid_image in enumerate(pyramid):
            new_file_name = f"{level}.tif"
            new_file_path = output_dir / new_file_name
            # if overwriting is off this will always be a new dir, no need to
            # check if the image exists before overwriting
            with tifffile.TiffWriter(new_file_path) as fp:
                fp.write(
                    pyramid_image.astype(np.uint16), description=description
                )

            url = BASE_URL + self.to_server_path(new_file_path)
            leveldict[int(level)] = renderapi.image_pyramid.MipMap(url)
            description = None  # don't add the description to all of them

        return renderapi.image_pyramid.ImagePyramid(leveldict)

    def create_all_mipmaps(self) -> typing.List[Stack]:
        """create all mipmaps and write them

        returns list of stacks
        """
        futures = set()
        all_sections = {}
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.parallel
        )
        try:
            for args in self.find_files():
                future = executor.submit(self.create_mipmaps, args)
                futures.add(future)

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                desc="making mipmaps",
                total=len(futures),
                unit="img",
                smoothing=min(100 / len(futures), 0.3),
            ):
                futures.remove(future)
                tiles = future.result()
                for tile in tiles:
                    stack = all_sections.setdefault(tile.stackname, {})
                    try:
                        section = stack[tile.zvalue]
                    except KeyError:
                        section = stack[tile.zvalue] = Section(
                            tile.zvalue, tile.stackname
                        )

                    section.add_tile(tile)
        finally:
            for future in futures:
                future.cancel()

            executor.shutdown()

        all_stacks = []
        for name, sections in all_sections.items():
            stack = Stack(name)
            for section in sections.values():
                stack.add_section(section)

            all_stacks.append(stack)

        count = sum(len(stack.tilespecs) for stack in all_stacks)
        logging.info(
            f"created {len(all_stacks)} stacks containing {count} tiles"
        )
        return all_stacks

    @abc.abstractmethod
    def find_files(self):
        """generator that finds all the files to read in self.project_path

        yields args for create_mipmaps
        """

    @abc.abstractmethod
    def create_mipmaps(self, args):
        """create mipmaps for a file

        args: result yielded from find_files
        """
