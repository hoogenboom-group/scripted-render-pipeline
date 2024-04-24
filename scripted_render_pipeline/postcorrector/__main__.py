"""post-correction script for FAST-EM datasets

relies on provided parameters being set in the script for now
"""
import logging
import pathlib
from natsort import natsorted
from .post_corrector import Post_Corrector

# script properties
PROJECT = "20231212_MC7_NdAc"
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = True  # set to false to fail if data would be overwritten
REMOTE = False  # set to false if ran locally
NAS_SHARE_PATH = pathlib.Path.home() / "shares/long_term_storage"
SERVER_STORAGE_PATH_STR = "/long_term_storage/"
PROJECT_PATH = (
    NAS_SHARE_PATH if REMOTE else pathlib.Path(SERVER_STORAGE_PATH_STR)
) / f"akievits/FAST-EM/{PROJECT}"
MULTIPLE_SECTIONS = True  # Set to False for a single section

# Processing parameters
PCT = 0.1
A = 3

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
