"""importer script
Imports data and metadata into render-ws
Makes MIPmaps

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
OWNER = "thopp"
PROJECT = "SK0002_H004_A001_subdataset"

# script properties
MIPMAP_TYPE = "CLEM"  # which mipmapper to use, either FASTEM or CLEM
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = True  # set to false to fail if data would be overwritten
Z_RESOLUTION = 100  # the thickness of sections
REMOTE = True  # set to false if ran locally
# the location the nas is mounted on this machine, only used when remote
NAS_SHARE_PATH = pathlib.Path.home() / "shares/long_term_storage"
SERVER_STORAGE_PATH_STR = "/long_term_storage/"  # Base storage path
# Path to data
PROJECT_PATH_STR = "skaracoban/test_data/SK0002_H004_A001_subdataset"
MIPMAP_PATH_STR = "thopp/SK0002_H004_A001_subdataset_mipmaps"
if REMOTE:
    PROJECT_PATH = NAS_SHARE_PATH / PROJECT_PATH_STR
    MIPMAP_PATH = NAS_SHARE_PATH / MIPMAP_PATH_STR
else:
    PROJECT_PATH = pathlib.Path(SERVER_STORAGE_PATH_STR) / PROJECT_PATH_STR
    MIPMAP_PATH = pathlib.Path(SERVER_STORAGE_PATH_STR) / MIPMAP_PATH_STR

# use the automated stitching results from acquisition software
IMPORT_TFORMS = False

# for FAST-EM datasets only
MULTIPLE_SECTIONS = True  # Set to False for a single section
CORRECTIONS_DIR = "postcorrection"

if MULTIPLE_SECTIONS:
    project_paths = []
    for path in PROJECT_PATH.iterdir():
        if path.is_dir() and not path.name.startswith("_"):
            project_paths.append(path / CORRECTIONS_DIR)
    project_paths = natsorted(project_paths)
else:
    project_paths = None


def _main():
    auth = load_auth()
    match MIPMAP_TYPE:
        case "CLEM":
            mipmapper = CLEM_Mipmapper(
                PROJECT_PATH,
                PARALLEL,
                CLOBBER,
                import_tforms=IMPORT_TFORMS,
                mipmap_path=MIPMAP_PATH
            )
        case "FASTEM":
            mipmapper = FASTEM_Mipmapper(
                PROJECT_PATH,
                PARALLEL,
                CLOBBER,
                import_tforms=IMPORT_TFORMS,
                project_paths=project_paths,
                mipmap_path=MIPMAP_PATH
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
