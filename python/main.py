import logging
import queue
import time
from argparse import ArgumentParser
from functools import partial
from multiprocessing import Pool, Queue, Manager
from typing import Iterator, List

import pandas as pd
import rasterio

from indexraster import s2indexing
from python.indexraster.s2utils import s2_id_to_c_int

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

parser = ArgumentParser()
parser.add_argument(
    "--inputPath", help="Raster file to read from", required=True
)
parser.add_argument("--outPath", help="Output csv path", required=True)
parser.add_argument("--nodata", help="Nodata value for raster files.", required=False, default=0.0)
parser.add_argument("--s2lvl", help="S2 level to use for indexing.", required=False, default=11)
parser.add_argument("--returnGeom", help="Return geometry in output. Y or N", required=False, default=True)

args = parser.parse_args()


def process_block(block: s2indexing.RasterBlockData, params: s2indexing.IndexingParams, out_queue: Queue):
    cells = s2indexing.get_block_cells(block, params)
    t1 = time.perf_counter()
    for cell in cells:
        row = cell._asdict()
        row["s2_id"] = s2_id_to_c_int(cell.s2_id)
        out_queue.put(row, False)
    t2 = time.perf_counter()
    logger.info(f"Processed {len(cells)} cells in {t2 - t1} seconds")


def main():
    return_geom = args.returnGeom.lower() == "y"
    params = s2indexing.IndexingParams(
        lvl=int(args.s2lvl),
        nodata=float(args.nodata),
        return_geom=return_geom
    )

    m = Manager()
    q = m.Queue()
    with rasterio.open(args.inputPath) as src:
        block_data = generate_blocks(src)

        out_data = []
        with Pool() as p:
            process = partial(process_block, params=params, out_queue=q)
            p.map(process, block_data, chunksize=10)
            while True:
                try:
                    out_data.append(q.get(False, timeout=10))
                except queue.Empty:
                    break

    out_df = pd.DataFrame(out_data).groupby("s2_id").agg(
        {"value": "sum", "geom": "first"})
    out_df.to_csv(args.outPath, index=False)


def generate_blocks(src) -> s2indexing.RasterBlockData:
    for ji, window in src.block_windows(1):
        block = src.read(1, window=window)
        transform = src.window_transform(window)
        block_data = s2indexing.RasterBlockData(block, transform)
        yield block_data


if __name__ == "__main__":
    main()
    print("Done!")
