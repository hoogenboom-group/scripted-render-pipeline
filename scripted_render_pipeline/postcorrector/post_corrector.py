import concurrent.futures
import logging
import shutil

import numpy as np
import random
import tifffile
from skimage.transform import pyramid_gaussian
from tqdm import tqdm
from itertools import zip_longest

SCOPE_ID = "FASTEM"
IM_SIZE = 6400 
METADATA_FILENAME = "mega_field_meta_data.yaml"
POSITIONS_FILENAME = "positions.txt"
CORRECTIONS_DIR = "corrected"
POST_CORRECTIONS_DIR = "postcorrection"
IMAGE_FILENAME_PADDING = 3
TIFFILE_GLOB = (
    "[0-9]" * IMAGE_FILENAME_PADDING
    + "_"
    + "[0-9]" * IMAGE_FILENAME_PADDING
    + "_0.tiff"
)
SAMPLE_SIZE = 20


class Post_Corrector:
    """Applies post-correction of FAST-EM datasets to remove acquisition artifacts

    project_path: path to project to do post-corrections for
    parallel: how many threads to use in parallel, optimises io usage
    clobber: wether to allow overwriting of existing mipmaps
    mipmap_path: where to save mipmaps, defaults to project_path/_mipmaps

    additional optional named arguments:
    project_paths: iterable of multiple project paths, indexed by zlevel
    use_positions: use the transforms from the positions.txt file
    """

    def __init__(
        self,
        project_path,
        parallel=1,
        clobber=False,
        project_paths=None,
    ):
        self.project_path = project_path
        self.parallel = parallel
        self.clobber = clobber
        if project_paths is not None:
            self.project_paths = project_paths

    def post_correct_all_sections(self):
        """create post-corrected images for all sections"""
        
        futures = set()
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.parallel
        )

        try:
            for filepaths in self.find_files():
                future = executor.submit(self.post_correct_section, filepaths)
                futures.add(future)

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                desc="post-correcting sections",
                total=len(futures),
                unit="section",
                smoothing=min(100 / len(futures), 0.3),
            ):
                futures.remove(future)
                future.result()
        finally:
            for future in futures:
                future.cancel()

            executor.shutdown()

    def post_correct_section(self, filepaths: list):
        """create post_corrected images for one section

        filepaths: list of filepaths of raw images in sections
        """
        # Sample N images from each section
        fp_sample = random.sample(filepaths, SAMPLE_SIZE) 
    
        # Initialize median computation array
        median_im = np.zeros((IM_SIZE, IM_SIZE, SAMPLE_SIZE))
        
        # Open and append
        for i, file_path in enumerate(fp_sample):
            with tifffile.TiffFile(file_path) as tiff:
                if not tiff.pages:
                    raise RuntimeError(f"found empty tifffile: {file_path}")
                median_im[:, :, i] = tiff.pages[0].asarray()
                
        # Create post-corrected image based on median of images per pixel
        median_im = np.median(median_im, axis=2)
        # Correct
        self.post_correct(filepaths, median_im)

    def post_correct(self, filepaths: list, median_im: np.array):
        """Reapply post-processing corrections to images

        Parameters
        ----------

        filepaths : Filepaths to raw images in one section
        fps_clean : List of filepaths of artefact-free fields
        """

        # Set target output directory
        post_correction_dir = filepaths[0].parent / POST_CORRECTIONS_DIR
        post_correction_dir.mkdir(parents=True, exist_ok=self.clobber)
        # Copy metadata because render_import requires it
        shutil.copyfile(
            filepaths[0].parent / METADATA_FILENAME,
            post_correction_dir / METADATA_FILENAME,
        )

        for file_path in filepaths:
            with tifffile.TiffFile(file_path) as tiff:
                image = tiff.pages[0].asarray()
                n_layers = len(tiff.pages)
                # Subtract background from each raw field
                # and restore to 16bit mean level
                post_corrected = (
                    image - median_im + median_im.min()
                ).astype(np.uint16)
                # Save corrected field as pyramidal tiff
                filepath_corrected = post_correction_dir / file_path.name
                self.save_pyramidal_tiff(
                    filepath_corrected, post_corrected, n_layers=n_layers
                )
        # Save background
        self.save_pyramidal_tiff(
            post_correction_dir / "median_of_files.tiff",
            median_im.astype(np.uint16),
            None,
        )

    def save_pyramidal_tiff(
        self, filepath, image, metadata=None, n_layers=5, options=None
    ):
        """Save image as multi-page, pyramidal tiff

        Parameters
        ----------
        filepath : `pathlib.Path`
            Filepath to save tiff
        image : array-like
            Input image, becomes the base-level of the pyramid
        metadata : dict
            Tiff metadata
        n_layers : int (optional)
            Number of layers
        options : dict (optional)
            Extra optional metadata
        """
        # Generate image pyramid
        pyramid = pyramid_gaussian(
            image,
            downscale=2,
            order=3,
            max_layer=n_layers,
            preserve_range=True,
        )
        # Handle metadata
        if metadata is None:
            metadata = {}

        # save pyramid and force uint16
        with tifffile.TiffWriter(filepath) as writer:
            for data in pyramid:
                writer.write(
                    np.array(data, dtype=np.uint16),
                    metadata=metadata,
                    photometric="minisblack",
                    # predictor=True,
                    # compression="zlib",
                    # compressionargs={"level": 6},
                )

    def find_files(self):
        try:
            num_sections = len(self.project_paths)
        except AttributeError:
            num_sections = 1
        logging.info(
                f"reading data from {num_sections} section(s) using "
                f"{min(self.parallel, num_sections)} threads"
                )
        try:
            iterator = self.project_paths
        except AttributeError:
            iterator = [self.project_path]
        filepaths_per_section = (
            list(path.glob(TIFFILE_GLOB)) for path in iterator
        )
        for filepaths in filepaths_per_section:
            yield filepaths  # Yields list of filepaths per section