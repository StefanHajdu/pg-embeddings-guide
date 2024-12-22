import os
import pandas as pd
import time
import sys
import socket
import json
import multiprocessing

from dotenv import load_dotenv
from itertools import batched

from db_utils import open_sqlalchemy_conn
from pg_embed_server import launch_server
from measure_utils import measure


NUM_PARALLEL_WORKERS = 4


class EmbedSocketPool:
    def __init__(self, num_workers=1):
        self.uds_paths = []
        self.processes = []
        self.num_workers = num_workers
        self.mp_context = multiprocessing.get_context("spawn")
        self._create_uds_files()

    def _create_uds_files(self):
        for i in range(self.num_workers):
            if not os.path.exists(f"/tmp/uds_{i}.socket"):
                os.mknod(f"/tmp/uds_{i}.socket")
            self.uds_paths.append(f"/tmp/uds_{i}.socket")

    def spawn_servers(self):
        for i in range(self.num_workers):
            p = self.mp_context.Process(
                target=launch_server, args=[f"/tmp/uds_{i}.socket"]
            )
            p.start()
            self.processes.append(p)

    def __str__(self):
        s = ""
        for p in self.processes:
            s += f"{p}\n"
        return s


def send_chunks_to_sockets(uds_pool, bulk):
    batches = list(batched(bulk, 1))

    for i in range(len(batches)):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        # make sure server is listening!
        s.connect(uds_pool.uds_paths[i])
        s.sendall(json.dumps(batches[i][0]).encode("utf-8"))
        s.close()


@measure
def iterate_and_update(iterator_conn, uds_pool, limit=2_500):
    cnt = 0
    with iterator_conn.connect() as conn:
        for df in pd.read_sql(
            "SELECT idx::text, text FROM youtube_comments",
            conn,
            chunksize=NUM_PARALLEL_WORKERS,
        ):
            if cnt == limit:
                break
            else:
                cnt += 1
                bulk = df.to_dict(orient="records")
                send_chunks_to_sockets(uds_pool, bulk)


if __name__ == "__main__":
    load_dotenv()
    iterator_conn = open_sqlalchemy_conn()

    uds_pool = EmbedSocketPool(NUM_PARALLEL_WORKERS)
    uds_pool.spawn_servers()
    print(uds_pool)

    print("Servers running.")
    time.sleep(10)
    iterate_and_update(iterator_conn, uds_pool)
