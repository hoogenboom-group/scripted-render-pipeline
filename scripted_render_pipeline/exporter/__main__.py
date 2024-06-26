"""exporter script
Export datasets to WebKnossos

relies on provided parameters being set in the script for now
"""

import logging
import renderapi
import requests

from pathlib import Path
from .webknossos_exporter import Webknossos_Exporter
from ..basic_auth import load_auth

# Export parameters
HOST = "https://sonic.tnw.tudelft.nl" # Web address which hosts render-ws. It's usually the preamble of the link to render-ws html page, i.e. {host_name}/render-ws/view/index.html
OWNER = "akievits" # render-ws ID of dataset
PROJECT = "20231107_MCF7_UAC_test" # Project name in render-ws
STACKS_2_EXPORT = ["postcorrection_rigid_scaled"]  # Python list of stacks (strings) in render-ws to export (separated by commas)
DOWNSCALING = 1  # Downscale data for testing. Default is 1 (normal resolution)
DOWNSAMPLE = 7  # How many times to downsample data
CONCURRENCY = 8  # Default number of processes to use
PATH = Path(
    f"/long_term_storage/webknossos/binaryData/hoogenboom-group/{PROJECT}" 
) # Path to target WebKnossos folder, replace with target directory on disk


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
