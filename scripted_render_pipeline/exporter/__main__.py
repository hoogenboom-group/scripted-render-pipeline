"""exporter script

relies on provided parameters being set in the script for now
"""
import logging
import pathlib

from ..basic_auth import load_auth
from .connecter import Connecter
from .CATMAID_exporter import CATMAID_Exporter
from .WK_exporter import WK_Exporter
from .utils import ExportTarget

# render properties
HOST = "https://sonic.tnw.tudelft.nl"
OWNER = "skaracoban"
PROJECT = "20240219_PD05_final_test"
STACKS_2_EXPORT = ["exc_405nm_correlated_new"]  # list
# STACKS_2_EXPORT = ["exc_405nm_correlated", "EM_himag_world"]  # list
# STACKS_2_EXPORT = ["EM_himag_stitched"]  # list
CLIENT_SCRIPTS = "/home/catmaid/render/render-ws-java-client/src/main/scripts"
WK_CLIENT_SCRIPT = "/opt/webknossos/tools/cube.sh"

# script properties
PARALLEL = 40  # read this many images in parallel to optimise io usage
CLOBBER = True  # set to false to fail if data would be overwritten
# Z_RESOLUTION = 100  # DEPRICATED: thickness of sections should be in render-ws already
REMOTE = False  # set to false if ran locally
# set to True if CATMAID directory should be removed (only when exporting to WebKnossos)
REMOVE_CATMAID_DIR = False
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
    # Connect to     render-ws
    connecter = Connecter(HOST, OWNER, PROJECT, auth)
    RENDER = connecter.get_render_info()

    match EXPORT_TYPE:
        case ExportTarget.WEBKNOSSOS:
            exporter = WK_Exporter(WK_DIR, CATMAID_DIR, RENDER, CLIENT_SCRIPTS,
                                   WK_CLIENT_SCRIPT, PARALLEL, CLOBBER, REMOVE_CATMAID_DIR)
        case ExportTarget.CATMAID:
            exporter = CATMAID_Exporter(
                CATMAID_DIR, RENDER, CLIENT_SCRIPTS, PARALLEL, CLOBBER)
        case _:
            raise RuntimeError(f"Export format not supported! '{EXPORT_TYPE}'")

    exporter.export_stacks(STACKS_2_EXPORT)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()
