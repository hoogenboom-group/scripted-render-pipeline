"""stitching script

relies on provided parameters being set in the script for now
"""
import logging

from ..basic_auth import load_auth
from .stitch import Stitcher

# render properties
host = "https://sonic.tnw.tudelft.nl"
owner = "thopp"
# project = "20240515_MDCK_small"
# project = "20240723_testis_100nm"
# project = "20240627_MDCK"
project = "SK0002_H004_A001_subdataset"
# stack = "raw"
# stack = "corrected"
stack = "EM_himag"


def _main():
    auth = load_auth()
    stitcher = Stitcher(host, owner, project, stack, auth, True)
    stitcher.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()
