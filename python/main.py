import logging
import queue
from argparse import ArgumentParser
from functools import partial
from multiprocessing import Pool, Queue, Manager
from typing import Iterable

from tqdm import tqdm
import ibis
import numpy as np
import rasterio
import scipy.stats

from indexraster import s2indexing
from indexraster.s2utils import s2_id_to_c_int

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

parser = ArgumentParser()
parser.add_argument("--inputPath", help="Raster file to read from", required=True)
parser.add_argument("--outPath", help="Output csv path", required=True)
parser.add_argument(
    "--nodata", help="Nodata value for raster files.", required=False, default=0.0
)
parser.add_argument(
    "--s2lvl", help="S2 level to use for indexing.", required=False, default=11
)
parser.add_argument(
    "--returnGeom",
    help="Return geometry in output.",
    action="store_true",
    required=False,
    default=True,
)
parser.add_argument(
    "--chunkSize",
    help="Chunk size for processing.",
    required=False,
    default=10,
)

args = parser.parse_args()


def process_block(
    block: s2indexing.RasterBlockData,
    params: s2indexing.IndexingParams,
    out_queue: Queue,
):
    cells = s2indexing.get_block_cells(block, params)
    row_group = []
    for cell in cells:
        row = cell._asdict()
        row["s2_id"] = s2_id_to_c_int(cell.s2_id)
        row_group.append(row)
    out_queue.put(row_group, False)


def main():
    ibis.options.default_backend = "duckdb"
    return_geom = args.returnGeom
    params = s2indexing.IndexingParams(
        lvl=int(args.s2lvl), nodata=float(args.nodata), return_geom=return_geom
    )

    m = Manager()
    q = m.Queue()
    with rasterio.open(args.inputPath) as src:
        block_x, block_y = src.profile.get("blockxsize"), src.profile.get("blockysize")
        if block_x is None or block_y is None:
            block_x, block_y = src.width, 1
        total_blocks = round((src.height * src.width) / (block_x * block_y))
        block_data = generate_blocks(src)

        out_data = []
        with Pool() as p:
            process = partial(process_block, params=params, out_queue=q)
            results = tqdm(
                p.imap(process, block_data, chunksize=int(args.chunkSize)),
                total=total_blocks,
            )

            for res in results:
                try:
                    row_group = q.get(False, timeout=10)
                    for row in row_group:
                        out_data.append(row)
                    if res:
                        print(res)
                except queue.Empty:
                    break

    print("Processing finished, aggregating and writing...")

    out_df = ibis.memtable(out_data, columns=["s2_id", "value", "geometry"]).aggregate(
        by=["s2_id"],
        value=lambda x: x.value.mode(),
        geometry=lambda x: x.geometry.first(),
    )
    print(out_df)
    out_df.to_parquet(args.outPath)


def generate_blocks(src) -> Iterable[s2indexing.RasterBlockData]:
    for _, window in src.block_windows(1):
        block = src.read(1, window=window)
        transform = src.window_transform(window)
        block_data = s2indexing.RasterBlockData(block, transform)
        yield block_data


if __name__ == "__main__":
    main()
    print("Done!")
