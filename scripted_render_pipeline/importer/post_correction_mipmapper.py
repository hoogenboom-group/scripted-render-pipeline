import logging

import numpy as np
import tifffile

from .fastem_mipmapper import FASTEM_Mipmapper


RESTORE_MEAN_LEVEL = 0x8000


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
        self.min_clean_images = 20

    def set_medians(self):
        """calculate and set internal median and median absolute deviation"""
        percentiles = []
        for args in self.find_files():
            file_path, *_ = args
            with tifffile.TiffFile(file_path) as tiff:
                if not tiff.pages:
                    raise RuntimeError(f"found empty tifffile: {file_path}")

                image = tiff.pages[0].asarray()
                percentiles.append(np.percentile(image, self.percentile))

        self.median = np.median(percentiles)
        absolute_deviations = []
        for percentile in percentiles:
            absolute_deviations.append(percentile - self.median)

        self.median_absolute_deviation = np.median(absolute_deviations)

    def set_background(self):
        """calculate and set internal background image"""
        clean_image_count = 0
        sum_of_files = 0.0  # set to float to avoid integer overflow
        for args in self.find_files():
            file_path, *_ = args
            result = self.calculate_background_for_path(file_path)
            if result:
                sum_of_files += result
                clean_image_count += 1

        logging.info(f"found {clean_image_count} clean images")
        if clean_image_count < self.min_clean_images:
            raise RuntimeError(
                "amount of clean images is less than {self.min_clean_images}!"
            )

        self.background = sum_of_files / clean_image_count
        del self.sum_of_files

    def calculate_background_for_path(self, file_path):
        """add file at file_path to the calculation for the backgrIound

        returns the image if it is within acceptable limits or None
        """
        with tifffile.TiffFile(file_path) as tiff:
            if not tiff.pages:
                raise RuntimeError(f"found empty tifffile: {file_path}")

            image = tiff.pages[0].asarray()
            result = np.percentile(image, self.percentile)
            limit_range = self.threshold * self.median_absolute_deviation
            lower_limit = self.median - limit_range
            upper_limit = self.median + limit_range
            if lower_limit < result < upper_limit:
                return image
            else:
                return None

    def create_all_mipmaps(self):  # overwrite
        logging.info("calculating medians")
        self.set_medians()
        logging.info("calculating background")
        self.set_background()
        return super().create_all_mipmaps()

    def make_pyramid(self, output_dir, image, description):  # overwrite
        post_corrected = image - self.background + RESTORE_MEAN_LEVEL
        return super().make_pyramid(output_dir, post_corrected, description)
