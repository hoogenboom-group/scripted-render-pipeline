import collections
import concurrent.futures
import logging
import multiprocessing as mp

import renderapi
import requests
import tqdm

from . import match
from ..importer import uploader
from .get_match_tiles import get_match_tiles
from .montage import montage


DEBUG = False
SINGLE_LAYER = False


class Stitcher:
    """stitches images in xy"""

    def __init__(
        self,
        host,
        owner,
        project,
        stack,
        auth=None,
        clobber=False,
        max_workers=16,
    ):
        session = requests.Session()
        session.auth = auth
        self.render = dict(
            host=host,
            owner=owner,
            project=project,
            session=session,
        )
        self.stack = stack
        self.clobber = clobber
        self.max_workers = max_workers
        if DEBUG:
            self.max_workers = 1

        self.logger = logging.getLogger(self.__class__.__name__)
        # amount of pixels that the images overlap on the edges
        self.overlap = 400
        # intelligently remove keypoints over this limit
        self.max_keypoints = 400
        self.sift_params = dict(
            n_octaves=4,
            n_scales=3,
            sigma_min=2.6,
            sigma_in=0.5,
            # degrees of gaussian count
            # higher means higher required contrast ie less features
            c_dog=0.025,
            # edgeness threshold
            # lower means stricter threshold on edge like features
            c_edge=4.5,
        )
        self.match_params = dict(
            max_distance=self.overlap,
            cross_check=True,
            max_ratio=0.8,
        )
        self.ransac_params = dict(
            model_class=match.skimage.transform.EuclideanTransform,
            residual_threshold=6.2,  # max distance # epsilon
            max_trials=2134,
        )

    def log(self, *args, **kwargs):
        """log to class logger, for convenience"""
        self.logger.info(*args, **kwargs)

    def get_all_matches(self):
        """get all pointmatches in the entire stack

        returns a 2 tuple of all pointmatches indexed by z and all connections
        between tiles indexed by z
        """
        futures = {}
        ProcessPoolExecutor = concurrent.futures.ProcessPoolExecutor
        if DEBUG:
            ProcessPoolExecutor = concurrent.futures.ThreadPoolExecutor

        with mp.Manager() as manager:
            image_limit = manager.Lock()

            with ProcessPoolExecutor(self.max_workers) as pool:
                for zlevel in self.z_values:
                    for direction, tilepairs in enumerate(
                        self.matched_tiles[zlevel]
                    ):
                        for p_name, q_name, x, y, section_id in tilepairs:
                            matcher = match.Matcher(
                                self,
                                p_name,
                                q_name,
                                x,
                                y,
                                section_id,
                                match.ALIGNMENTS[direction],
                                zlevel,
                            )
                            future = pool.submit(
                                matcher.make_pointmatches,
                                image_limit,
                            )
                            futures[future] = matcher

            z_all_matches = collections.defaultdict(list)
            z_connections = collections.defaultdict(
                lambda: collections.defaultdict(list)
            )
            for future in tqdm.tqdm(
                concurrent.futures.as_completed(futures), total=len(futures)
            ):
                result = future.result()
                if result:
                    matcher = futures[future]
                    zlevel = matcher.zlevel
                    p_name = matcher.p_name
                    q_name = matcher.q_name
                    z_all_matches[zlevel].append(result)
                    z_connections[zlevel][p_name].append(q_name)
                    z_connections[zlevel][q_name].append(p_name)

        return z_all_matches, z_connections

    def filter_tilespecs(self, matches, connections):
        """filter the tilespecs in the stack

        downloads the original stack tilespecs and then reduces them to only
        the ones that are connected

        matches: all matches indexed by z
        connections: all connections between tiles indexed by z
        returns a 2 tuple of all filtered matches and valid tilespecs as lists
        """
        # get all tilespecs
        tilespec_list = renderapi.tilespec.get_tile_specs_from_stack(
            self.stack, **self.render
        )
        all_tilespecs = {spec.tileId: spec for spec in tilespec_list}

        matches_to_send = []
        good_tilespecs = []
        for zlevel in self.z_values:
            if zlevel not in matches:
                self.logger.warning(
                    f"zlevel {zlevel} does not have any matches"
                )
                continue

            level_matches = matches[zlevel]
            level_connections = connections[zlevel]
            groups = []
            while level_connections:
                connected = set()
                checking = [next(iter(level_connections))]
                while checking:
                    name = checking.pop()
                    connected.add(name)
                    for item in level_connections.pop(name):
                        if item not in connected and item not in checking:
                            checking.append(item)

                groups.append(connected)

            largest = 0
            for group in groups:
                if len(group) > largest:
                    largest_group = group
                    largest = len(group)

            for d in level_matches:
                if d["pId"] in largest_group or d["qId"] in largest_group:
                    matches_to_send.append(d)

            for tile_id in largest_group:
                good_tilespecs.append(all_tilespecs[tile_id])

        return good_tilespecs, matches_to_send

    def upload_to_render(self, tilespecs, matches):
        """upload the new stack and matches to render"""
        # upload stack with only matching tiles
        metadata = renderapi.stack.get_stack_metadata(
            self.stack, **self.render
        )
        self.matching_stack = f"{self.stack}_matching"
        if self.clobber:
            try:
                renderapi.stack.delete_stack(
                    self.matching_stack, **self.render
                )
            except renderapi.errors.RenderError:
                pass

        renderapi.stack.create_stack(
            self.matching_stack,
            stackResolutionX=metadata.stackResolutionX,
            stackResolutionY=metadata.stackResolutionY,
            stackResolutionZ=metadata.stackResolutionZ,
            **self.render,
        )
        uploader.import_tilespecs(
            self.matching_stack, tilespecs, **self.render
        )
        renderapi.stack.set_stack_state(
            self.matching_stack, "COMPLETE", **self.render
        )
        self.log(f"uploaded {self.matching_stack}")

        # upload pointmatches
        self.matches_name = f"{self.render['project']}_{self.stack}_matches"
        if self.clobber:
            try:
                renderapi.pointmatch.delete_collection(
                    self.matches_name, **self.render
                )
            except renderapi.errors.RenderError as exc:
                if exc.args[0].endswith("does not exist"):
                    self.log("no existing pointmatches to overwrite")
                else:
                    # log warning for now, if it's critical we'll error on
                    # the upload next
                    self.logger.warning(
                        "did not delete existing pointmatches", exc_info=exc
                    )

        renderapi.pointmatch.import_matches(
            self.matches_name, matches, **self.render
        )
        self.log(f"uploaded {self.matches_name}")

    def montage(self):
        """runs the montage after pointmatching completed

        requires uploading first
        """
        self.stitched_stack = montage(
            self.matching_stack, self.matches_name, self.render, self.clobber
        )
        self.log(f"created {self.stitched_stack}")

    def run(self):
        """runs the stitcher from start to finish"""
        self.z_values = [
            int(zlevel)
            for zlevel in renderapi.stack.get_z_values_for_stack(
                self.stack, **self.render
            )
        ]
        if SINGLE_LAYER:
            get = len(self.z_values) // 4  # "randomly" select a layer
            self.z_values = [self.z_values[get]]

        self.log(f"got {len(self.z_values)} layers")

        self.matched_tiles, self.size = get_match_tiles(
            self.stack, self.z_values, self.render
        )
        matches, connections = self.get_all_matches()
        tilespecs, matches = self.filter_tilespecs(matches, connections)
        self.upload_to_render(tilespecs, matches)
        self.montage()
        self.log(f"stitching for {self.stack} completed")
