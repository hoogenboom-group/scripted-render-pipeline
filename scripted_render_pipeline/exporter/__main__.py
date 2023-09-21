"""exporter script

relies on provided parameters being set in the script for now
"""
import logging
import pathlib

from ..basic_auth import load_auth
from .connecter import Connecter
from .CATMAID_exporter import CATMAID_Exporter
from .WK_exporter import WK_Exporter

# render properties
HOST = "https://sonic.tnw.tudelft.nl"
OWNER = "rlane"
PROJECT = "20230523_singleholder_Earthworm_03_partial_partial_test"
STACKS_2_EXPORT = "ROA_1"
CLIENT_SCRIPTS = pathlib.Path.home() / "catmaid/render/render-ws-java-client/src/main/scripts"

# script properties
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = True  # set to false to fail if data would be overwritten
# Z_RESOLUTION = 100  # DEPRICATED: thickness of sections should be render-ws already
REMOTE = False  # set to false if ran locally
NAS_SHARE_PATH = pathlib.Path.home() / "shares/long_term_storage"
SERVER_STORAGE_PATH_STR = "/long_term_storage/"
EXPORT_TYPE = "WEBKNOSSOS"  # "WEBKNOSSOS" or "CATMAID"

# export directories
CATMAID_DIR = (
    (NAS_SHARE_PATH if REMOTE else pathlib.Path(SERVER_STORAGE_PATH_STR))
    / f"catmaid_projects/{OWNER}/{PROJECT}"
)
WK_DIR = (
    (NAS_SHARE_PATH if REMOTE else pathlib.Path(SERVER_STORAGE_PATH_STR))
    / f"webknossos/binaryData/hoogenboom-group/{PROJECT}"
)

def _main():
    auth = load_auth()
    connecter = Connecter(HOST, OWNER, PROJECT, CLIENT_SCRIPTS, auth) # Connect to render-ws
    render_kwargs = Connecter.make_kwargs()

    match EXPORT_TYPE:
        case "WEBKNOSSOS":
            exporter = WK_Exporter(PROJECT, WK_DIR, PARALLEL, CLOBBER)
        case "CATMAID":
            exporter = CATMAID_Exporter(CATMAID_DIR, PARALLEL, CLOBBER, **render_kwargs)
        case _:
            raise RuntimeError(f"Export format not supported! '{EXPORT_TYPE}'")
    exporter.export_stacks(STACKS_2_EXPORT)
        

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()
