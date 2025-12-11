import collections
import logging
import pathlib

import renderapi
import skimage
import numpy as np

PQ = "p", "q"
ALIGNMENTS = "horizontal", "vertical"
SHOW = False
SHOW_DIR = pathlib.Path("imgs_stitch")

if SHOW:
    from matplotlib import pyplot as plt

    SHOW_DIR.mkdir(exist_ok=True)
    for path in SHOW_DIR.iterdir():
        if path.is_file():
            path.unlink()
            logging.debug(f"deleted {path}")


class Matcher:
    """matcher for one tilepair

    stitcher: instantiating stitcher to take params from
    p_name: name of frame
    q_name: name of other frame
    match_x: world x coordinate of border minimum
    match_y: world y coordinate of border minimum
    section_id: render section_id
    direction: horizontal or vertical
    zlevel: index of z layer
    """

    def __init__(
        self,
        stitcher,
        p_name,
        q_name,
        match_x,
        match_y,
        section_id,
        direction,
        zlevel,
        clahe=False,
    ):
        self.overlap = stitcher.overlap
        self.max_keypoints = stitcher.max_keypoints
        self.sift_params = stitcher.sift_params
        self.match_params = stitcher.match_params
        self.ransac_params = stitcher.ransac_params
        self.size = stitcher.size
        self.stack = stitcher.stack
        self.render = stitcher.render

        self.p_name = p_name
        self.q_name = q_name
        self.names = self.p_name, self.q_name
        self.match_x = match_x
        self.match_y = match_y
        self.section_id = section_id
        self.direction = ALIGNMENTS.index(direction)
        self.zlevel = zlevel
        self.clahe = clahe

        self.logger = logging.getLogger(self.__class__.__name__)

    def __repr__(self):
        s = f"<{ALIGNMENTS[self.direction]} {self.__class__.__name__}"
        s += f" between {' and '.join(self.names)}>"
        return s

    def log(self, *args, **kwargs):
        """log to class logger, for convenience"""
        self.logger.info(*args, **kwargs)

    def get_images(self, image_limit):
        """download the image from render

        returns the image
        """
        if self.direction == 0:
            boundary_box = (
                self.match_x - self.overlap,
                self.match_y,
                self.overlap * 2,
                self.size,
            )
        else:
            boundary_box = (
                self.match_x,
                self.match_y - self.overlap,
                self.size,
                self.overlap * 2,
            )

        with image_limit:  # be patient with renderapi
            img = renderapi.image.get_bb_image(
                self.stack, self.zlevel, *boundary_box, **self.render
            )

        if type(img) is renderapi.errors.RenderError:
            raise img  # do their job for them

        if self.direction == 0:
            imgs = img[:, : self.overlap, 0], img[:, self.overlap :, 0]
        else:
            imgs = img[: self.overlap, :, 0], img[self.overlap :, :, 0]

        self.log(f"got image from {boundary_box} on {self.section_id}")

        if not self.clahe:
            return imgs  # skip CLAHE

        # run CLAHE
        new_imgs = []
        for split_img, name in zip(imgs, self.names):
            # try not to enhance noise in the image
            blurry = skimage.filters.gaussian(split_img, 2)
            new_imgs.append(
                skimage.exposure.equalize_adapthist(
                    blurry, self.overlap / 16, clip_limit=0.02
                )
            )
            self.log(f"ran adaptive contrast enhance on {name}")

        return new_imgs

    def filter_keypoints(self, sift):
        """filter keypoints for each bin

        sift: result from sift
        """
        keep = np.zeros_like(sift.sigmas, dtype=bool)  # default to remove
        pool = collections.defaultdict(lambda: collections.defaultdict(list))
        for i, (coord, sigma) in enumerate(zip(sift.keypoints, sift.sigmas)):
            x_coord, y_coord = coord
            pool[x_coord // self.overlap][y_coord // self.overlap].append(
                (sigma, i)
            )

        total_pools = sum(len(x_pool) for x_pool in pool.values())
        kp_per_pool = self.max_keypoints // total_pools
        for x_pool in pool.values():
            for y_pool in x_pool.values():
                y_pool.sort()
                for sigma, index in y_pool[:kp_per_pool]:
                    keep[index] = True

        return keep

    def make_pointmatches(self, image_limit):
        """make the pointmatches for each pair

        returns pointmatches for this tilepair as dict
        """
        imgs = self.get_images(image_limit)

        # run SIFT
        sift = skimage.feature.SIFT(1, **self.sift_params)
        keypoints = []
        descriptors = []
        for split_img, porq, name in zip(imgs, PQ, self.names):
            try:
                sift.detect_and_extract(split_img)
            except RuntimeError:
                self.log(f"SIFT found no features in {name}")
                self.log(f"{self} will not have enough matches")
                return {}

            amount = len(sift.descriptors)
            self.log(f"found {amount} {porq} descriptors on {name}")
            keep = self.filter_keypoints(sift)
            self.log(
                f"filtered from {len(sift.keypoints)} to {sum(keep)} {porq} "
                f"descriptors on {name}"
            )
            if not keep.any():
                self.log(f"{name} doesn't have any {porq} descriptors left")
                self.log(f"{self} will not have enough matches")
                return {}

            keypoints.append(sift.keypoints[keep])
            descriptors.append(sift.descriptors[keep])

        # run match
        matches = skimage.feature.match_descriptors(
            *descriptors, **self.match_params
        )
        del descriptors  # descriptors are no longer needed

        self.log(f"{self} found {len(matches)} matches")
        matched_keypoints = [
            kpts[matches[:, i]] for i, kpts in enumerate(keypoints)
        ]

        # run RANSAC
        filtered_matches_amount = len(matches)
        min_samples = max(round(filtered_matches_amount * 0.05), 7)
        if (
            filtered_matches_amount < min_samples
        ):  # NOTE this just compares to 7
            self.log(f"{self} does not have enough matches")
            return {}

        transform, inliers = skimage.measure.ransac(
            matched_keypoints, min_samples=min_samples, **self.ransac_params
        )
        if inliers is None:
            self.log(f"{self} could not fit matches")
            return {}

        total_inliers = sum(inliers)
        if total_inliers < min_samples:
            self.log(
                f"{self} could not find enough inliers: "
                f"{total_inliers}/{min_samples}"
            )
            return {}

        self.log(f"{self} filtered to {total_inliers} matches")

        if SHOW:  # save matched images as plot
            axes = plt.subplot()
            skimage.feature.plot_matches(
                axes,
                *imgs,
                *keypoints,
                matches[inliers],
                alignment=ALIGNMENTS[self.direction],
            )
            plt.savefig(
                SHOW_DIR.joinpath(f"{'_x_'.join(self.names)}.png"), dpi=1000
            )

        filtered = [mkp[inliers] for mkp in matched_keypoints]

        adjusted = self.size - self.overlap
        p_result = [], []
        q_result = [], []
        for (py, px), (qy, qx) in zip(*filtered):
            # add to direction coord on p_result to place it on the edge
            if self.direction == 0:
                px += adjusted
            else:
                py += adjusted

            p_result[0].append(px)
            p_result[1].append(py)
            q_result[0].append(qx)
            q_result[1].append(qy)

        return dict(
            pGroupId=self.section_id,
            qGroupId=self.section_id,
            pId=self.p_name,
            qId=self.q_name,
            matches=dict(
                p=p_result,
                q=q_result,
                w=[1 for _ in p_result[0]],
            ),
        )
