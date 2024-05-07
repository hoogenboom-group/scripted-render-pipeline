"""exporter script

relies on provided parameters being set in the script for now
"""

import logging
import renderapi
import requests

from pathlib import Path
from .webknossos_exporter import Webknossos_Exporter
from ..basic_auth import load_auth

# Export parameters
HOST = "https://sonic.tnw.tudelft.nl"
OWNER = "akievits"
PROJECT = "20231107_MCF7_UAC_test"
STACKS_2_EXPORT = ["postcorrection_rigid_scaled"]  # list
DOWNSCALING = 1  # Default
DOWNSAMPLE = 7  # How many times to downsample data
CONCURRENCY = 8  # Default number of processes
PATH = Path(
    "/long_term_storage/webknossos/binaryData/hoogenboom-group/20231107_MCF7_UAC_test_test"
)


def _main():
    # Render authentication
    USER, PASSWORD = load_auth()
    session = requests.Session()
    session.auth = (USER, PASSWORD)
    render = dict(host=HOST, owner=OWNER, project=PROJECT, session=session)

    # Get voxel size
    # Assumes equal voxel sizes
    # TODO: expand for stacks with different voxel sizes
    stack_metadata = [
        renderapi.stack.get_stack_metadata(stack, **render)
        for stack in STACKS_2_EXPORT
    ]
    voxel_size = (
        stack_metadata[0].stackResolutionX,
        stack_metadata[0].stackResolutionY,
        stack_metadata[0].stackResolutionZ,
    )

    wk_exporter = Webknossos_Exporter(
        location=PATH,
        host=HOST,
        owner=OWNER,
        project=PROJECT,
        downscaling=DOWNSCALING,
        voxel_size=voxel_size,
        downsample=DOWNSAMPLE,
        concurrency=CONCURRENCY,
    )
    wk_exporter.download_project(stacks=STACKS_2_EXPORT)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()
