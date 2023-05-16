import logging
import pathlib

from .basic_auth import load_auth
from .mipmapper import Mipmapper
from .uploader import Uploader

# render properties
HOST = "https://sonic.tnw.tudelft.nl"
OWNER = "rlane"
PROJECT = "20191101_ratpancreas_partial_partial_test"

# script properties
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = True  # set to false to fail if data would be overwritten
Z_RESOLUTION = 100  # the thickness of sections
REMOTE = True  # set to false if ran locally
NAS_SHARE_PATH = pathlib.Path.home() / "shares/long_term_storage"
SERVER_STORAGE_PATH_STR = "/long_term_storage/"
PROJECT_PATH = (
    (NAS_SHARE_PATH if REMOTE else pathlib.Path(SERVER_STORAGE_PATH_STR))
    / "thopp/20191101_rat-pancreas_partial"
    #  / "rlane/SECOM/projects/20191101_rat-pancreas_partial"
)


def _main():
    auth = load_auth()
    mipmapper = Mipmapper(PROJECT_PATH, PARALLEL, CLOBBER)
    if REMOTE:
        mipmapper.set_remote_path(NAS_SHARE_PATH, SERVER_STORAGE_PATH_STR)

    stacks = mipmapper.create_all_mipmaps()
    if not stacks:
        raise RuntimeError("no stacks to upload")

    uploader = Uploader(HOST, OWNER, PROJECT, auth, CLOBBER)
    uploader.upload_to_render(stacks, Z_RESOLUTION)
    logging.info("import completed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()
