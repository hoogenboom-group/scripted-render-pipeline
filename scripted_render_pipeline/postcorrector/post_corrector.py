import logging
import shutil

import numpy as np
import tifffile

import concurrent.futures
import logging

import numpy as np
from tqdm import tqdm

from PIL import Image
from skimage.transform import pyramid_gaussian

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


class Post_Corrector():
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
        self, project_path, parallel=1, clobber=False, pct=0.1, a=1, project_paths=None
    ):
        self.project_path = project_path
        self.parallel = parallel
        self.clobber = clobber
        self.pct = pct
        self.a = a
        if project_paths is not None:
            self.project_paths = project_paths

    def post_correct_all_sections(self):
        """create post-corrected images for all sections

        """
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
                unit="sections",
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
        fps_clean = []
        med = self.get_med(filepaths, pct=self.pct)
        mad = self.get_mad(filepaths, med=med, pct=self.pct)
        
        # Determine non-corrupted images 
        for file_path in filepaths:
            with tifffile.TiffFile(file_path) as tiff:
                if not tiff.pages:
                    raise RuntimeError(f"found empty tifffile: {file_path}")
                image = tiff.pages[0].asarray()
                corrupted = self.has_artefact(image, med=med, mad=mad, 
                                              pct=self.pct, a=self.a)
                if not corrupted:
                    fps_clean.append(file_path)
        
        # Create post-corrected images based on non-corrupted images
        self.post_correct(filepaths, fps_clean)
        
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
        corrupted = ((p1 < med - a*mad) | (p1 > med + a*mad))
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
        shutil.copyfile(filepaths[0].parent / METADATA_FILENAME, post_correction_dir / METADATA_FILENAME)

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
                metadata = tiff.pages[0].tags
                n_layers = len(tiff.pages) 
                # Subtract background from each raw field
                # and restore to 16bit mean level
                post_corrected = (image - background + RESTORE_MEAN_LEVEL).astype(np.uint16)
                # Save corrected field as pyramidal tiff
                filepath_corrected = post_correction_dir / file_path.name
                self.save_pyramidal_tiff(filepath_corrected, post_corrected, n_layers=n_layers)
        # Save background
        self.save_pyramidal_tiff(post_correction_dir / 'sum_of_files.tiff', background.astype(np.uint16), None)  

    def save_pyramidal_tiff(self, filepath, image, metadata=None, n_layers=5, options=None):
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

        References
        ----------
        [1] https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html#saving-tiff-images
        """
        # Generate image pyramid
        pyramid = pyramid_gaussian(image,
                                   downscale=2,
                                   order=3,
                                   max_layer=n_layers,
                                   preserve_range=True)
        # Extract layers from pyramid and force uint16
        layers = [Image.fromarray(layer.astype(np.uint16)) for layer in pyramid]
        # Handle metadata
        if metadata is None:
            metadata = {}
        im = layers[0]
        im.save(filepath.as_posix(), append_images=layers[1:],
                tiffinfo=metadata, save_all=True)
        
    def find_files(self):  # override
        logging.info(
            f"reading data from {len(self.project_paths)} section(s) using "
            f"{min(self.parallel, len(self.project_paths))} threads"
        )
        try:
            iterator = self.project_paths.items()
        except AttributeError:
            iterator = self.project_paths
        except ValueError:
            iterator = self.project_path
        filepaths_per_section = (list(path.glob(TIFFILE_GLOB)) for path in iterator)
        for filepaths in filepaths_per_section:
                yield filepaths # Yields list of filepaths per section