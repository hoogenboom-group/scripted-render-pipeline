import logging

import renderapi
import requests

from .basic_auth import load_auth

# render properties
HOST = "https://sonic.tnw.tudelft.nl"
OWNER = "rlane"
PROJECT = "20191101_ratpancreas_partial_partial_test"
# http basic auth info
USER, PASSWORD = load_auth()


def _main():
    session = requests.Session()
    session.auth = (USER, PASSWORD)
    # this replaces the render.connect or render.Render!
    render = dict(host=HOST, owner=OWNER, project=PROJECT, session=session)
    # note that instead of using render=render we now simply unpack this dict
    existing_stacks = renderapi.render.get_stacks_by_owner_project(**render)
    joined = ", ".join(existing_stacks)
    logging.info(f"{PROJECT} contains {len(existing_stacks)} stacks: {joined}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    _main()
