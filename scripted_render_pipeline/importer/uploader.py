import logging
import typing

import renderapi
import requests
from tqdm import tqdm

from .render_specs import Stack


# TileSpec.to_dict does not include boundary box properties, so we need to add
# them otherwise we can't use deriveData=False which will perform the boundary
# box calculation separately, which is unnecessary because we already have the
# boundary box size here
def _tilespec_to_dict(ts):
    d = ts.to_dict()
    d["minX"] = ts.minX
    d["minY"] = ts.minY
    d["maxX"] = ts.maxX
    d["maxY"] = ts.maxY
    return d


# subclass from ResolvedTiles that uses our _tilespec_to_dict
class _resolved_tiles(renderapi.resolvedtiles.ResolvedTiles):
    def to_dict(self):
        d = {
            "transformIdToSpecMap": {
                tf.transformId: tf.to_dict() for tf in self.transforms
            },
            "tileIdToSpecMap": {
                ts.tileId: _tilespec_to_dict(ts) for ts in self.tilespecs
            },
        }
        return d


def import_tilespecs(stack, tilespecs, shared_transforms=None, **kwargs):
    """calling import_tilespec with the rest api

    instead of using client scripts we use the api directly, this way no java
    is required
    deriveData will also be set to False, this requires the tiles to add their
    size to the dict sent to the server
    """
    # resolve tiles beforehand using subclassed ResolvedTiles for deriveData
    resolvedtiles = _resolved_tiles(
        tilespecs=tilespecs, transformList=shared_transforms
    )

    # call unwrapped function to avoid client scripts check,
    # this is pretty hacky
    renderapi.client.import_tilespecs.__wrapped__(
        stack=stack,
        tilespecs=None,
        resolved_tiles=resolvedtiles,
        use_rest=True,
        deriveData=False,
        **kwargs,
    )


class Uploader:
    """talks with the render-ws rest api to upload stacks

    host: address of the server hosting render-ws
    owner: name of the render project owner
    project: render project name
    auth: http basic auth credentials, tuple of (username, password)
    clobber: whether to allow overwriting of existing projects
    """

    def __init__(self, host, owner, project, auth=None, clobber=False):
        session = requests.Session()
        session.auth = auth
        self.render = dict(
            host=host, owner=owner, project=project, session=session
        )
        self.host = host
        self.owner = owner
        self.project = project
        self.clobber = clobber

    def upload_to_render(
        self, stacks: typing.Iterable[Stack], z_resolution=100
    ):
        """upload a list of stacks to render"""
        existing_stacks = renderapi.render.get_stacks_by_owner_project(
            **self.render
        )
        logging.info(
            f"uploading {len(stacks)} stacks to {self.host} for {self.owner} "
            f"in {self.project}"
        )
        for stack in tqdm(stacks, desc="uploading", unit="stacks"):
            if stack.name in existing_stacks:
                if self.clobber:
                    logging.warn(f"overwriting {stack.name} in {self.project}")
                    renderapi.stack.delete_stack(stack.name, **self.render)
                else:
                    raise RuntimeError(
                        f"stack {stack.name} already exists in project "
                        f"{self.project}"
                    )

            pixel_size = stack.pixel_size
            renderapi.stack.create_stack(
                stack.name,
                stackResolutionX=pixel_size,
                stackResolutionY=pixel_size,
                stackResolutionZ=z_resolution,
                **self.render,
            )
            import_tilespecs(
                stack.name,
                stack.tilespecs,
                **self.render,
            )
            renderapi.stack.set_stack_state(
                stack.name, "COMPLETE", **self.render
            )
