from enum import Enum


class ExportTarget(str, Enum):
    """Supported export targets."""
    CATMAID = "CATMAID"
    WEBKNOSSOS = "WEBKNOSSOS"
