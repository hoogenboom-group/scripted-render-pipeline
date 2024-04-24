"""importer script

relies on provided parameters being set in the script for now
"""
import logging
import pathlib
from natsort import natsorted

from ..basic_auth import load_auth
from .clem_mipmapper import CLEM_Mipmapper
from .fastem_mipmapper import FASTEM_Mipmapper
from .uploader import Uploader

# render properties
HOST = "https://sonic.tnw.tudelft.nl"
OWNER = "akievits"
PROJECT = "20231107_MCF7_UAC_test"
CORRECTIONS_DIR = "postcorrection"

# script properties
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = True  # set to false to fail if data would be overwritten
Z_RESOLUTION = 100  # the thickness of sections
REMOTE = False  # set to false if ran locally
NAS_SHARE_PATH = pathlib.Path.home() / "shares/long_term_storage"
SERVER_STORAGE_PATH_STR = "/long_term_storage/"
PROJECT_PATH = (
    (NAS_SHARE_PATH if REMOTE else pathlib.Path(SERVER_STORAGE_PATH_STR))
    # / "thopp/20191101_rat-pancreas_partial"
    #  / "rlane/SECOM/projects/20191101_rat-pancreas_partial"
    / f"akievits/FAST-EM/tests/{PROJECT}"
)
MIPMAP_TYPE = "FASTEM"  # "CLEM"
# for fastem datasets only
USE_POSITIONS = True  # use the automated stitching results
MULTIPLE_SECTIONS = True # Set to False for a single section

if MULTIPLE_SECTIONS:
    PROJECT_PATHS = natsorted([p / CORRECTIONS_DIR for p in PROJECT_PATH.iterdir() if (p.is_dir() and not p.name.startswith('_'))])

   
def _main():
    auth = load_auth()
    match MIPMAP_TYPE:
        case "CLEM":
            mipmapper = CLEM_Mipmapper(PROJECT_PATH, PARALLEL, CLOBBER)
        case "FASTEM":
            mipmapper = FASTEM_Mipmapper(
                PROJECT_PATH, PARALLEL, CLOBBER, use_positions=USE_POSITIONS, project_paths=PROJECT_PATHS
            )
        case _:
            raise RuntimeError(f"wrong mipmap type! '{MIPMAP_TYPE}'")

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
