"""post-correction script for FAST-EM datasets

relies on provided parameters being set in the script for now
"""
import logging
import pathlib

from .post_corrector import Post_Corrector

# script properties
PROJECT = "20230914_RP_exocrine_partial_test"
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = False  # set to false to fail if data would be overwritten
REMOTE = False  # set to false if ran locally
NAS_SHARE_PATH = pathlib.Path.home() / "shares/long_term_storage"
SERVER_STORAGE_PATH_STR = "/long_term_storage/"
PROJECT_PATH = (
    NAS_SHARE_PATH if REMOTE else pathlib.Path(SERVER_STORAGE_PATH_STR)
) / f"akievits/FAST-EM/{PROJECT}"
MULTIPLE_SECTIONS = True  # Set to False for a single section

# Processing parameters
PCT = 0.1
A = 1

if MULTIPLE_SECTIONS:
    PROJECT_PATHS = [p for p in PROJECT_PATH.iterdir() if p.is_dir()]
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

    post_corrector.post_correct_all_sections()
    logging.info("post-correction completed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()
