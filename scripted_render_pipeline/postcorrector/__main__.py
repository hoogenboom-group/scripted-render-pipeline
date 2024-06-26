"""post-correction script for FAST-EM datasets
Computes average correction image per section
Outlier images are excluded from average based on median absolute deviation
Produces a directory "postcorrection" with corrected images per section

relies on provided parameters being set in the script for now
"""
import logging
import pathlib
from natsort import natsorted
from .post_corrector import Post_Corrector

# script properties
PROJECT = "20231107_MCF7_UAC_test" # Project folder name on disk
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = True  # set to false to fail if data would be overwritten
REMOTE = False  # set to false if ran locally
NAS_SHARE_PATH = pathlib.Path.home() / "shares/long_term_storage"
SERVER_STORAGE_PATH_STR = "/long_term_storage/" # Base storage path
PROJECT_PATH = (
    NAS_SHARE_PATH if REMOTE else pathlib.Path(SERVER_STORAGE_PATH_STR)
) / f"akievits/FAST-EM/tests/{PROJECT}" # Path to data 
MULTIPLE_SECTIONS = True  # Set to False for a single section (assumes that project folder contains data and there are no subdirectories for individual sections)

# Processing parameters
PCT = 0.1 # Histogram percentile for computing Median Absolute Deviation (MAD)
A = 3 # Scaling factor for allowing smaller or larger deviations from the MED. Suggested values: 1, 3

if MULTIPLE_SECTIONS:
    PROJECT_PATHS = natsorted([p for p in PROJECT_PATH.iterdir() if (p.is_dir() and not p.name.startswith('_'))])
else:
    PROJECT_PATHS = None


def _main():
    post_corrector = Post_Corrector(
        PROJECT_PATH,
        PARALLEL,
        CLOBBER,
        pct=PCT,
        a=A,
        project_paths=PROJECT_PATHS,
    )

    failed_sections = post_corrector.post_correct_all_sections()
    if not failed_sections:
        logging.info("post-correction completed succesfully")
    else:
        logging.info("Post_correction failed for: %s", [section.name for section in failed_sections])      
        logging.info("Detected failed sections. Rerunning post-correction using nearest available correction image")
        post_corrector.post_correct_failed_sections(failed_sections)



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()