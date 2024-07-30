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
RESTORE_MEAN_LEVEL = 32768
SAMPLE_SIZE = 10
MIN_CLEAN = 20


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
        pct=0.1,
        a=1,
        project_paths=None,
    ):
        self.project_path = project_path
        self.parallel = parallel
        self.clobber = clobber
        self.pct = pct
        self.a = a
        if project_paths is not None:
            self.project_paths = project_paths

    def post_correct_all_sections(self):
        """create post-corrected images for all sections"""
        # Sample N images from each section
        all_paths = []
        for filepaths in self.find_files():
            fp_sample = random.sample(filepaths, SAMPLE_SIZE)
            all_paths += fp_sample   
        # Compute MED and MAD from global sample
        logging.info("Estimating global med and mad values")
        med = self.get_med(all_paths, pct=self.pct)
        mad = self.get_mad(all_paths, med=med, pct=self.pct)
        # Compute correction per section
        futures = set()
        failed_sections = []
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.parallel
        )

        try:
            for filepaths in self.find_files():
                future = executor.submit(self.post_correct_section, filepaths, med, mad)
                futures.add(future)

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                desc="post-correcting sections",
                total=len(futures),
                unit="section",
                smoothing=min(100 / len(futures), 0.3),
            ):
                futures.remove(future)
                failed_section = future.result()
                failed_sections += failed_section
        finally:
            for future in futures:
                future.cancel()

            executor.shutdown()
    
        return failed_sections
        
    def post_correct_failed_sections(self, failed_sections):
        """create post-corrected images for all sections that failed initial post-correction"""
        # Compute correction for failed sections
        futures = set()
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(self.parallel, len(failed_sections))
        )
        filepaths_per_section = (
            list(path.glob(TIFFILE_GLOB)) for path in failed_sections
        )
        try:
            for filepaths in filepaths_per_section:
                future = executor.submit(
                    self.post_correct_failed_section(filepaths)
                    )
                futures.add(future)

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                desc="post-correcting failed sections",
                total=len(futures),
                unit="section",
                smoothing=min(100 / len(futures), 0.3),
            ):
                futures.remove(future)
                
        finally:
            for future in futures:
                future.cancel()

            executor.shutdown()
        

    def post_correct_section(self, filepaths: list, med, mad):
        """create post_corrected images for one section

        filepaths: list of filepaths of raw images in sections
        med: Median Deviation of percentiles
        mad: Median Absolute Deviation of percentiles
        """
        fps_clean = []
        # Determine non-corrupted images
        for file_path in filepaths:
            with tifffile.TiffFile(file_path) as tiff:
                if not tiff.pages:
                    raise RuntimeError(f"found empty tifffile: {file_path}")
                image = tiff.pages[0].asarray()
                corrupted = self.has_artefact(
                    image, med=med, mad=mad, pct=self.pct, a=self.a
                )
                if not corrupted:
                    fps_clean.append(file_path)
        # Create post-corrected images based on non-corrupted images 
        # only if sufficient number of clean images is available
        if len(fps_clean) > MIN_CLEAN:
            self.post_correct(filepaths, fps_clean)
            return []
        else:
            return [filepaths[0].parent] # Path to failed section
            
    def get_med(self, filepaths, pct=1):
        """Get median value of given percentile of select images"""
        # Collect percentile values
        ps = []
        # Loop through tiffs
        for file_path in filepaths:
            with tifffile.TiffFile(file_path) as tiff:
                if not tiff.pages:
                    raise RuntimeError(f"found empty tifffile: {file_path}")
                # Read tiff and extract lowest resolution page from pyramid
                image = tiff.pages[-1].asarray()
                # Compute percentile
                p1 = np.percentile(image, pct)
                ps.append(p1)
        # Compute median
        med = np.median(ps)
        return med

    def get_mad(self, filepaths, med, pct=1):
        """Get median absolute deviation from given percentile of select images

        References
        ----------
        [1] https://en.wikipedia.org/wiki/Median_absolute_deviation
        """
        # Collect absolute deviations
        ads = []
        # Loop through tiffs
        for file_path in filepaths:
            # Read tiff and extract lowest resolution page from pyramid
            tiff = tifffile.TiffFile(file_path.as_posix())
            image = tiff.pages[-1].asarray()
            # Compute absolute deviation
            p1 = np.percentile(image, pct)
            ad = np.abs(p1 - med)
            ads.append(ad)
        # Compute median absolute deviation
        mad = np.median(ads)
        return mad

    def has_artefact(self, image, med: float, mad: float, pct=1, a=3):
        """Determine if image contains an artefact based on intensity percentiles

        image: Input image
        med: Median `pct`-percentile value of megafield
        mad: Median absolute deviation from `pct`-percentile across megafield
        pct: Percentile
        a: Scaling factor for thresholding the deviation from the median
            Increasing `a` will allow for larger deviations

        Returns
        -------
        corrupted: Bool.
            Whether image has been corrupted by an artefact
        """
        p1 = np.percentile(image, pct)
        corrupted = (p1 < med - a * mad) | (p1 > med + a * mad)
        return corrupted

    def post_correct(self, filepaths: list, fps_clean: list):
        """Reapply post-processing corrections to images

        Parameters
        ----------

        filepaths : Filepaths to raw images in one section
        fps_clean : List of filepaths of artefact-free fields
        """
        sum_of_files = 0.0  # set to float to avoid integer overflow

        # Set target output directory
        post_correction_dir = filepaths[0].parent / POST_CORRECTIONS_DIR
        post_correction_dir.mkdir(parents=True, exist_ok=self.clobber)
        # Copy metadata because render_import requires it
        shutil.copyfile(
            filepaths[0].parent / METADATA_FILENAME,
            post_correction_dir / METADATA_FILENAME,
        )

        # Estimate background by averaging over clean images
        for file_path in fps_clean:
            with tifffile.TiffFile(file_path) as tiff:
                if not tiff.pages:
                    raise RuntimeError(f"found empty tifffile: {file_path}")
                image = tiff.pages[0].asarray()
                # Sum all the clean images together
                sum_of_files += image

        # Make the sum a mean
        background = sum_of_files / len(fps_clean)

        for file_path in filepaths:
            with tifffile.TiffFile(file_path) as tiff:
                image = tiff.pages[0].asarray()
                n_layers = len(tiff.pages)
                # Subtract background from each raw field
                # and restore to 16bit mean level
                post_corrected = (
                    image - background + RESTORE_MEAN_LEVEL
                ).astype(np.uint16)
                # Save corrected field as pyramidal tiff
                filepath_corrected = post_correction_dir / file_path.name
                self.save_pyramidal_tiff(
                    filepath_corrected, post_corrected, n_layers=n_layers
                )
        # Save background
        self.save_pyramidal_tiff(
            post_correction_dir / "sum_of_files.tiff",
            background.astype(np.uint16),
            None,
        )
    
    def post_correct_failed_section(self, filepaths: list):
        """Reapply post-processing corrections to images without correction
        Correction image is used from nearest section

        Parameters
        ----------

        filepaths : Filepaths to raw images in one section
        """
        # Set target (section) output directory
        section_dir = filepaths[0].parent
        post_correction_dir = section_dir / POST_CORRECTIONS_DIR
        post_correction_dir.mkdir(parents=True, exist_ok=self.clobber)
        
        # Copy metadata because render_import requires it
        shutil.copyfile(
            section_dir / METADATA_FILENAME,
            post_correction_dir / METADATA_FILENAME,
        )
        # Hacky way to fetch background image from nearest section 
        # starting with the section index below, then alterating 
        # sections further above or below the section
        s_i = self.project_paths.index(section_dir) # Section index
        lower = self.project_paths[:s_i][::-1] # Reverse 
        higher = self.project_paths[s_i+1:]
        paths_2_search = [item for pair in zip_longest(lower, higher) for item in pair] # Pads with NoneType elements
        paths_2_search = [path for path in paths_2_search if path is not None]
            
        # Iterate through nearest adjacent images
        for dir in paths_2_search:
            fp_correction = dir / POST_CORRECTIONS_DIR / "sum_of_files.tiff"
            try: 
                background = tifffile.imread(fp_correction.as_posix()) # Fails if image does not exists
            except FileNotFoundError:
                continue
            else:
                break
        # Iterate through filepaths and perform correction
        for file_path in filepaths:
            with tifffile.TiffFile(file_path) as tiff:
                image = tiff.pages[0].asarray()
                n_layers = len(tiff.pages)
                # Subtract background from each raw field
                # and restore to 16bit mean level
                post_corrected = (
                    image - background + RESTORE_MEAN_LEVEL
                ).astype(np.uint16)
                # Save corrected field as pyramidal tiff
                filepath_corrected = post_correction_dir / file_path.name
                self.save_pyramidal_tiff(
                    filepath_corrected, post_corrected, n_layers=n_layers
                )
        # Save background copy
        self.save_pyramidal_tiff(
            post_correction_dir / "sum_of_files.tiff",
            background.astype(np.uint16),
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
            iterator = self.project_paths.items()
        except AttributeError:
            iterator = [self.project_path]
        filepaths_per_section = (
            list(path.glob(TIFFILE_GLOB)) for path in iterator
        )
        for filepaths in filepaths_per_section:
            yield filepaths  # Yields list of filepaths per section
