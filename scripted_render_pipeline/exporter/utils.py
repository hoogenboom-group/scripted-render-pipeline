from enum import Enum


class ExportTarget(str, Enum):
    """Supported export targets."""
    CATMAID = "CATMAID".casefold()
    WEBKNOSSOS = "WEBKNOSSOS".casefold()
