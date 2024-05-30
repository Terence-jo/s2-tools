import logging

import numpy as np
import s2geometry
from s2geometry import S2LatLng, S2CellId, S2Cell
from shapely.geometry import Polygon, Point
from typing import List, Tuple

logger = logging.getLogger(__name__)


def s2_id_to_c_int(s2_id: int) -> np.int64:
    """Converts an S2 cell ID to a numpy int64. This is necessary for
    compatibility with both Arrow-based IO and BigQuery.
    A near-maximum uint64 won't fit in an int64, and so won't write
    to parquet as an int64, but a near-max uint64 in Parquet also will
    not read into BigQuery, thus the double-cast."""
    return np.int64(np.uint64(s2_id))


def s2_to_shapely(cell_id: S2CellId) -> Polygon:
    """S2CellId constructor does not accept numpy.int64. If instantiating a S2CellId
    to pass to this funciton and operating on IDs from a pandas df cast
    to int first."""
    logger.debug(f"cell_id: {type(cell_id)}")
    vertices = [S2LatLng(S2Cell(cell_id).GetVertex(v)) for v in range(4)]
    points = [Point(v.lng().degrees(), v.lat().degrees()) for v in vertices]
    logger.debug(f"points: {points}")
    return Polygon(points)


def s2_to_children(s2index: int, s2lvl: int) -> List[int]:
    """Get child cell ids for a given level."""
    children = S2CellId(s2index).children(s2lvl)
    return [child.id() for child in children]


def s2_rect_from_bounds(
    bounds: Tuple[float, float, float, float]
) -> s2geometry.S2LatLngRect:
    bounds = enforce_bounds_validity(bounds)

    return s2geometry.S2LatLngRect(
        s2geometry.S2LatLng.FromDegrees(bounds[1], bounds[0]),
        s2geometry.S2LatLng.FromDegrees(bounds[3], bounds[2]),
    )


def enforce_bounds_validity(
    bounds: Tuple[float, float, float, float]
) -> Tuple[float, float, float, float]:
    if bounds[0] <= -180:
        return -179.999999, bounds[1], bounds[2], bounds[3]
    if bounds[0] >= 180:
        return 179.999999, bounds[1], bounds[2], bounds[3]
    if bounds[2] <= -180:
        return bounds[0], bounds[1], -179.999999, bounds[3]
    if bounds[2] >= 180:
        return bounds[0], bounds[1], 179.999999, bounds[3]
    else:
        return bounds
