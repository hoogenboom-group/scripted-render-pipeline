import logging

import renderapi


class Tile:
    """object representing a single tile with one tilespec

    does not contain the image

    stackname: name of the stack
    zvalue: z index of the stack, matches section
    spec: TileSpec object
    acquisitiontime: time of acquisition, used to prioritise render order
    axes: Axis for each x and y
    min_intensity: lowest intensity in the image
    max_intensity: highest intensity in the image
    """

    def __init__(
        self,
        stackname,
        zvalue,
        spec,
        acquisitiontime,
        axes,
        min_intensity,
        max_intensity,
    ):
        self.stackname = stackname
        self.zvalue = zvalue
        self.spec = spec
        self.acquisitiontime = acquisitiontime
        self.axes = axes
        self.min_intensity = min_intensity
        self.max_intensity = max_intensity
        logging.debug(f"created tile: {self}")

    def __repr__(self):
        args = (
            self.stackname,
            self.zvalue,
            self.spec,
            self.acquisitiontime,
            self.axes,
            self.min_intensity,
            self.max_intensity,
        )
        argstr = ", ".join([repr(arg) for arg in args])
        return f"{self.__class__.__name__}({argstr})"

    def calculate_tform(self, min_position):
        min_pos = []
        max_pos = []
        origin_pos = []
        for min_value, axis in zip(min_position, self.axes):
            min_pos.append(axis.min_pos - min_value)
            max_pos.append(axis.max_pos - min_value)
            origin_pos.append(axis.pixel_position - min_value)

        spec = self.spec
        spec.minX, spec.minY = min_pos
        spec.maxX, spec.maxY = max_pos
        x_pos, y_pos = origin_pos
        model = renderapi.transform.AffineModel(B0=x_pos, B1=y_pos)
        spec.tforms = [model]


class Axis:
    """object representing the properties of a tile on a single axis

    box_min: min position in pixels of the image after transforms
    box_max: max position in pixels of the image after transforms
    position: physical location of top left corner on this axis
    pixel_size: size of a pixel in the same unit as position
    """

    def __init__(self, box_min, box_max, position, pixel_size=1):
        self.pixel_position = position / pixel_size
        self.min_pos = self.pixel_position + box_min
        self.max_pos = self.pixel_position + box_max

    def __repr__(self):
        args = (self.min_pos, self.max_pos, self.pixel_position)
        argstr = ", ".join([repr(arg) for arg in args])
        return f"{self.__class__.__name__}({argstr})"


class Section:
    """object representing a section of a single z level

    contains the Tile objects describing images, part of a stack
    does not contain the images

    zvalue: z level this section belongs to
    name: stack identifier this section belongs to
    """

    def __init__(
        self,
        zvalue,
        name,
        axes=None,
        tiles=None,
        intensity_range=None,
        pixel_size=None,
    ):
        self.zvalue = zvalue
        self.name = name
        self.range = intensity_range
        self.pixel_size = pixel_size
        self.topleft = axes
        if tiles:
            self.tiles_by_acquisitiontime = tiles
        else:
            self.tiles_by_acquisitiontime = {}

    def __repr__(self):
        args = (
            self.zvalue,
            self.name,
            self.topleft,
            self.tiles_by_acquisitiontime,
            self.range,
            self.pixel_size,
        )
        argstr = ", ".join([repr(arg) for arg in args])
        return f"{self.__class__.__name__}({argstr})"

    def add_tile(self, tile: Tile):
        """add a Tile to the section

        tile: the tile to add
        raises ValueError if spec with the same acquisitiontime already exists
        """
        if tile.acquisitiontime in self.tiles_by_acquisitiontime:
            raise ValueError(f"{tile.acquisitiontime} already has a spec")

        pixel_size = tile.spec.layout.pixelsize
        if self.pixel_size and pixel_size != self.pixel_size:
            raise ValueError(
                f"{tile} has wrong pixel_size, {pixel_size} instead of "
                f"{self.pixel_size}"
            )
        else:
            self.pixel_size = pixel_size

        if self.topleft is None:
            self.topleft = [axis.min_pos for axis in tile.axes]
        else:
            self.topleft = [
                min(original, other.min_pos)
                for original, other in zip(self.topleft, tile.axes)
            ]

        tile.spec.z = self.zvalue
        self.tiles_by_acquisitiontime[tile.acquisitiontime] = tile
        intensity_range = tile.min_intensity, tile.max_intensity
        if self.range is None:
            self.range = intensity_range
        else:
            amount = len(self.tiles_by_acquisitiontime)
            multiplier = amount - 1
            new_range = []
            for current, add in zip(self.range, intensity_range):
                # update the average, weighted by the total amount of values
                summed = current * multiplier + add
                new_range.append(summed / amount)

            self.range = new_range

    def set_tileids(self):
        """set the tileids on all specs

        this has to be done after all specs are created
        """
        width = len(str(len(self.tiles_by_acquisitiontime)))
        # give the most recent image the lowest tileId
        gen = reversed(sorted(self.tiles_by_acquisitiontime.keys()))
        for i, time in enumerate(gen):
            spec = self.tiles_by_acquisitiontime[time].spec
            sequential_id = str(i).zfill(width)
            spec.tileId = f"{sequential_id}_{self.name}_{self.zvalue}"

    def set_minmax(self):
        """update the min and max intensity values on all tilespecs

        all values in the section will be set to be the same
        the min and max intensity values will be rounded to integers
        """
        minmax = [round(value) for value in self.range]
        for spec in self.get_specs():
            spec.minint, spec.maxint = minmax

    def set_transforms(self):
        for tile in self.tiles_by_acquisitiontime.values():
            tile.calculate_tform(self.topleft)

    def get_specs(self):
        """return a generator of all tilespecs in this section

        specs are unsorted, their ordering is not guaranteed
        """
        return (tile.spec for tile in self.tiles_by_acquisitiontime.values())


class Stack:
    """object representing a stack

    contains the TileSpecs describing images and the name of the stack
    does not contain the images
    """

    def __init__(self, name, specs=None, pixel_size=None):
        self.name = name
        self.pixel_size = pixel_size
        if specs:
            self.tilespecs = specs
        else:
            self.tilespecs = []

    def __repr__(self):
        args = self.name, self.tilespecs, self.pixel_size
        argstr = ", ".join([repr(arg) for arg in args])
        return f"{self.__class__.__name__}({argstr})"

    def add_section(self, section: Section):
        """finalise a section and add it to this stack"""
        if self.pixel_size and section.pixel_size != self.pixel_size:
            raise ValueError(
                "{section} has wrong pixel_size, expected {self.pixel_size}"
            )
        else:
            self.pixel_size = section.pixel_size

        section.set_tileids()
        section.set_minmax()
        section.set_transforms()
        for spec in section.get_specs():
            self.tilespecs.append(spec)
