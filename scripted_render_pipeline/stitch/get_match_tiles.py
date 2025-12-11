import collections
import itertools
import logging

import renderapi
import numpy as np

_bounds = collections.namedtuple(
    "Bounds", "min_x min_y min_z max_x max_y max_z"
)
_tile_bounds = collections.namedtuple(
    "TileBounds", "min_x min_y min_z max_x max_y max_z tile_id section_id"
)


def Bounds(
    minX,
    maxX,
    minY,
    maxY,
    maxZ=None,
    minZ=None,
    z=None,
    tileId=None,
    sectionId=None,
    **_,
):
    """container for render bounds"""
    if tileId is None and sectionId is None and z is None:
        return _bounds(minX, minY, minZ, maxX, maxY, maxZ)
    elif maxZ is None and minZ is None:
        return _tile_bounds(minX, minY, z, maxX, maxY, z, tileId, sectionId)


def bounds_to_bbox(bounds):
    """convert Bounds object to 2d bbox array containing all four corners"""
    it = itertools.product(
        [bounds.min_x, bounds.max_x], [bounds.min_y, bounds.max_y]
    )
    return np.array([*it], dtype=float)


def get_match_tiles(stack, z_values, render):
    """find the coordinates of the seams of connected tiles

    this assumes unaligned tiles of equal size that have been placed in a grid
    by the importer to render

    stack: name of unaligned stack
    z_values: list of z levels to fetch matched tiles for
    render: render parameter dict
    returns a dict of matched tiles and the size as tuple

    the matched tiles dict contains a tuple with the matches in the x direction
    and y direction as list with each match a tuple containing the tile ids of
    the two matches, followed by the x and y coordinate of the start of the
    seam, followed by the section_id; the dict is keyed with the z level

    >>> matched_tiles = {int: tuple([tuple(str, str, int, int, str)]) * 2}

    the size and direction can be used to get the ending coordinate of the seam
    """
    logger = logging.getLogger("stitch_match_tiles")
    matches = {}
    size = None
    for z in z_values:
        bounds = [
            Bounds(**tile)
            for tile in renderapi.stack.get_tilebounds_for_z(
                stack, z, **render
            )
        ]
        for tile in bounds:
            if size is None:
                size = tile.max_x - tile.min_x

            if (
                tile.max_x - tile.min_x != size
                or tile.max_y - tile.min_y != size
            ):
                raise RuntimeError(
                    f"tile {tile} at level {z} in stack {stack} is not the "
                    f"right size, expected {size}x{size}"
                )

        # index bounds based on top left corner
        x_dict = collections.defaultdict(dict)
        for tile in bounds:
            x_dict[tile.min_x][tile.min_y] = tile.tile_id

        xlen = len(x_dict)
        ylen = 0
        for y_dict in x_dict.values():
            ylen = max(ylen, len(y_dict))

        x_matches = []
        x_unmatched = 0
        for tile in bounds:
            x, y = tile.max_x, tile.min_y
            if x not in x_dict:
                x_unmatched += 1
                continue

            try:
                matched = x_dict[x][y]
            except KeyError:
                # this happens when sections aren't rectangles
                x_unmatched += 1
                continue

            x_matches.append((tile.tile_id, matched, x, y, tile.section_id))

        if not x_matches:
            raise RuntimeError(
                f"could not find any matches for stack {stack} level {z}"
            )

        if x_unmatched > ylen:
            logger.info(f"stack {stack} level {z} is not a rectangle")
            # this means max does not correspond with the next tile's min
            # the tiles are not set up as a grid

        if x_unmatched < ylen:
            raise RuntimeError(
                f"stack {stack} level {z} somehow matched more tiles than "
                "possible"
            )
            # this is impossible as far as I know

        y_matches = []
        y_unmatched = 0
        for tile in bounds:
            x, y = tile.min_x, tile.max_y
            assert x in x_dict
            y_dict = x_dict[x]
            try:
                matched = y_dict[y]
            except KeyError:
                y_unmatched += 1
                continue

            y_matches.append((tile.tile_id, matched, x, y, tile.section_id))

        if not y_matches:
            raise RuntimeError(
                f"could not find any matches for stack {stack} level {z}"
            )

        if y_unmatched > xlen:
            logger.info(f"stack {stack} level {z} is not a rectangle")
            # this means max does not correspond with the next tile's min
            # the tiles are not set up as a grid

        if y_unmatched < xlen:
            raise RuntimeError(
                f"stack {stack} level {z} somehow matched more tiles than "
                "possible"
            )
            # this is impossible as far as I know

        logger.info(
            f"at level {z} found {len(x_matches) + len(y_matches)} matches"
        )
        matches[z] = x_matches, y_matches

    return matches, size
