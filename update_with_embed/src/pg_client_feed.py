import os
import pandas as pd
import time
import sys
import socket
import json

from dotenv import load_dotenv
from itertools import batched

from db_utils import open_sqlalchemy_conn
from measure_utils import async_measure, measure


SOCK_FILE_1 = "/tmp/ipc_1.socket"
SOCK_FILE_2 = "/tmp/ipc_2.socket"


if not os.path.exists(SOCK_FILE_1) or not os.path.exists(SOCK_FILE_2):
    print(f"File {SOCK_FILE_1} doesn't exists")
    sys.exit(-1)


def send_chunks_to_sockets(bulk):
    batches = list(batched(bulk, 1))

    s1 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # make sure server is listening!
    s1.connect(SOCK_FILE_1)
    s2.connect(SOCK_FILE_2)

    print(f"to s1: {batches[0][0]}")
    print(f"to s2: {batches[1][0]}")

    s1.sendall(json.dumps(batches[0][0]).encode("utf-8"))
    s2.sendall(json.dumps(batches[1][0]).encode("utf-8"))

    s1.close()
    s2.close()


@measure
def iterate_and_update(iterator_conn, limit=10):
    cnt = 0
    with iterator_conn.connect() as conn:
        for df in pd.read_sql(
            "SELECT idx::text, text FROM youtube_comments", conn, chunksize=2
        ):
            s1 = time.perf_counter()
            if cnt == limit:
                break
            else:
                cnt += 1
                bulk = df.to_dict(orient="records")
                send_chunks_to_sockets(bulk)

                #  seq_update_pg_with_embed(pg_conn, bulk, responses)
                print(f"iteration: {cnt} in {time.perf_counter() - s1}\n")


if __name__ == "__main__":
    load_dotenv()
    iterator_conn = open_sqlalchemy_conn()
    iterate_and_update(iterator_conn)
