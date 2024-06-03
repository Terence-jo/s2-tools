import json
import logging
import math
import time
from typing import NamedTuple, Dict, List
import numpy as np
import rasterio
import rasterio.features
import affine
import s2sphere
from shapely.geometry import shape
import scipy.stats

from indexraster.s2utils import s2_id_to_c_int, s2_rect_from_bounds, s2_to_shapely

EARTH_RADIUS = 6371000


class RasterBlockData(NamedTuple):
    data: np.ndarray
    transform: affine.Affine


logger = logging.getLogger(__name__)


class S2CellData(NamedTuple):
    s2_id: int
    tags: Dict
    value: float
    area: float
    geom: str = None


class IndexingParams(NamedTuple):
    lvl: int
    nodata: float
    return_geom: bool


def get_block_cells(block: RasterBlockData, params: IndexingParams) -> List[S2CellData]:
    data, transform = block.data, block.transform
    block_origin = transform.c, transform.f
    block_res = transform.a
    block_bounds = rasterio.transform.array_bounds(
        data.shape[0], data.shape[1], transform
    )
    nodata_mask = data != params.nodata

    middle_row_lat = block_origin[1] - (data.shape[0] / 2) * block_res
    pixel_area = pixel_area_m2(middle_row_lat, block_res)

    s2_coverer = s2sphere.RegionCoverer()
    s2_coverer.min_level = params.lvl
    s2_coverer.max_level = params.lvl
    s2_coverer.max_cells = 100000

    try:
        s2_rect = s2_rect_from_bounds(block_bounds)
    except AssertionError:
        logger.error(f"Invalid bounds: {block_bounds}")
        return []

    covering = s2_coverer.get_covering(s2_rect)

    cell_pairs = [(s2_to_shapely(cell), s2_id_to_c_int(cell.id())) for cell in covering]
    try:
        cell_mask = rasterio.features.rasterize(
            cell_pairs,
            out_shape=data.shape,
            transform=transform,
            dtype=np.int64,
        )
    except Exception as e:
        logger.error(f"Error rasterizing cells: {e}")
        return []

    out_cells = []
    for cell_geom, cell_id in cell_pairs:
        # cell_geom = s2_to_shapely(cell_id)
        # cell_mask = rasterio.features.geometry_mask(
        #     [cell_geom],
        #     out_shape=data.shape,
        #     transform=transform,
        #     invert=True,
        #     all_touched=True,
        # )
        data_under_cell = data[(cell_mask == cell_id) & nodata_mask]
        if pixel_area > cell_geom.area:
            value = (
                scipy.stats.mode(data_under_cell, axis=None).mode.squeeze()
                * cell_geom.area
                / pixel_area
            )
        else:
            value = scipy.stats.mode(data_under_cell, axis=None).mode.squeeze()
        # counts = {
        #     val: np.count_nonzero(data_under_cell == val)
        #     for val in np.unique(data_under_cell)
        # }
        geom_string = json.dumps(shape(cell_geom).__geo_interface__)

        if params.return_geom:
            out_cells.append(
                S2CellData(
                    s2_id=cell_id,
                    tags={"lvl": params.lvl},
                    value=float(value),
                    area=cell_geom.area,
                    geom=geom_string,
                )
            )
        else:
            out_cells.append(
                S2CellData(
                    s2_id=cell_id,
                    tags={"lvl": params.lvl},
                    value=float(value),
                    area=cell_geom.area,
                )
            )
    return out_cells


def pixel_area_m2(lat: float, res: float) -> float:
    """Takes a latitude and pixel resolution in degrees,
    returns the area of a pixel in square metres."""
    pix_width = haversine_pix_width(lat, res)
    pix_height = math.radians(res) * EARTH_RADIUS
    return pix_width * pix_height


def haversine_pix_width(lat: float, res: float) -> float:
    """Takes a latitude and pixel resolution in degrees,
    returns the width of a pixel in metres."""
    lat, res = map(math.radians, (lat, res))
    a = math.cos(lat) ** 2 * math.sin(res / 2) ** 2
    return 2 * EARTH_RADIUS * math.asin(math.sqrt(a))
