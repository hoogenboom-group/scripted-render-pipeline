import logging

import numpy as np
import tifffile

from .fastem_mipmapper import FASTEM_Mipmapper


class Post_Correction_Mipmapper(FASTEM_Mipmapper):
    """creates mipmaps from images and collects tile specs for the fastem

    includes option to apply post correction, subtracting the background from
    all images

    currently globalizes over all sections (TODO)

    arguments are the same as FASTEM_Mipmapper with these additional arguments
    percentile:
    threshold:
    min_clean_images:
    """

    def __init__(
        self, *args, percentile=1, threshold=1, min_clean_images=20, **kwargs
    ):
        super().__init__(*args, **kwargs)
        try:
            low, high = percentile
        except (ValueError, TypeError):
            self.percentile = percentile, 100 - percentile
        else:
            self.percentile = low, high

        self.threshold = threshold
        self.min_clean_images = min_clean_images

    def get_percentile(self, args):
        file_path, *_ = args
        with tifffile.TiffFile(file_path) as tiff:
            if not tiff.pages:
                raise RuntimeError(f"found empty tifffile: {file_path}")

            image = tiff.pages[0].asarray()
            return np.percentile(image, self.percentile)

    def set_medians(self):
        """calculate and set internal median and median absolute deviation"""
        percentiles = []
        for percentile in self.threaded_read_files(
            self.get_percentile, "calculating percentiles"
        ):
            percentiles.append(percentile)

        self.median = np.median(percentiles, 0)
        absolute_deviations = []
        for percentile in percentiles:
            absolute = np.abs(percentile - self.median)
            absolute_deviations.append(absolute)

        self.median_absolute_deviation = np.median(absolute_deviations, 0)

    def set_background(self):
        """calculate and set internal background image"""
        clean_image_count = 0
        total_count = 0
        sum_of_files = 0.0  # set to float to avoid integer overflow
        for result in self.threaded_read_files(
            self.calculate_background_for_path, "validating and reading images"
        ):
            total_count += 1
            if result is not None:
                sum_of_files += result
                clean_image_count += 1

        logging.info(
            f"found {clean_image_count} clean images out of {total_count}"
        )
        if clean_image_count < self.min_clean_images:
            raise RuntimeError(
                f"amount of clean images is less than {self.min_clean_images}!"
                f"({clean_image_count})"
            )

        self.background = sum_of_files / clean_image_count
        self.background -= np.mean(self.background)  # balance around mean

    def calculate_background_for_path(self, args):
        """add file at file_path to the calculation for the background

        returns the image if it is within acceptable limits else None
        """
        file_path, *_ = args
        with tifffile.TiffFile(file_path) as tiff:
            if not tiff.pages:
                raise RuntimeError(f"found empty tifffile: {file_path}")

            image = tiff.pages[0].asarray()
            low, high = np.percentile(image, self.percentile)
            limit_range = self.threshold * self.median_absolute_deviation
            lower_limit = self.median[0] - limit_range[0]
            upper_limit = self.median[1] + limit_range[1]
            if lower_limit < low and high < upper_limit:
                return image
            else:
                # logging.info(f"image at {file_path} has been rejected")
                return None

    def create_all_mipmaps(self):  # overwrite
        logging.info("calculating medians")
        self.set_medians()
        logging.info("calculating background")
        self.set_background()
        logging.info("creating mipmaps")
        return super().create_all_mipmaps()

    def make_pyramid(self, output_dir, image, description):  # overwrite
        post_corrected = image - self.background
        post_corrected = post_corrected.astype(image.dtype)
        return super().make_pyramid(output_dir, post_corrected, description)

    def read_tiff(self, *args, **kwargs):  # overwrite
        pyramid, unused_percentile, *rest = super().read_tiff(*args, **kwargs)
        # use median percentile for all tiles
        return pyramid, self.median, *rest
