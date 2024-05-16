#!/bin/env python
"""this module manages http basic auth credentials

a stored file with credentials to use with http basic auth is stored and can be
loaded when connecting to render.
before they can be loaded they need to be stored using "save".
"""
import json
import pathlib

# saves file inside project, a better option would be to use something like
# platformdirs to find a local directory that will work in case this were to
# be installed to a system location
FILENAME = ".auth.json"
FILEPATH = pathlib.Path(__file__).parent.joinpath(FILENAME)


class AuthFileMissing(FileNotFoundError):
    """the file with http basic auth credentials could not be found"""


def load_auth():
    """get http basic auth info from file"""
    try:
        with FILEPATH.open() as fp:
            return tuple(json.load(fp))
    except FileNotFoundError:
        raise AuthFileMissing(
            f"could not find auth file at {FILEPATH}, create it with save_auth"
        ) from None


def save_auth(username, password):
    """save http basic auth info to file"""
    auth = username, password
    try:
        FILEPATH.touch()
        FILEPATH.chmod(0o600)  # make file private to owner
    except OSError:
        pass

    with FILEPATH.open("w") as fp:
        return json.dump(auth, fp)


def _main():
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="basic_auth",
        description="loads and saves http basic auth credentials",
    )
    parser.add_argument("action", choices=["show", "save"], nargs="?")
    parser.add_argument("-s", "--silent", action="store_true")
    args = parser.parse_args()

    if args.action == "save":
        import getpass

        if args.silent:
            username, password = input().split(":")
        else:
            username = input("username:\n")
            password = getpass.getpass("password:\n")

        save_auth(username, password)
    else:
        items = "username", "password"
        try:
            auth = load_auth()
        except AuthFileMissing:
            print(
                "failed to get stored auth file, create it with save",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.silent:
            print(":".join(auth))
        else:
            for item, value in zip(items, auth):
                print(f"{item}:")
                print(f"{value}")


if __name__ == "__main__":
    _main()
