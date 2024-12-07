from functools import partial
import os
import io
import psycopg2
import time
from dotenv import load_dotenv

import asyncio
from concurrent.futures import ThreadPoolExecutor

from db_utils import async_measure, create_table, insert_into
from my_types import Row
from data_utils import get_data_as_tuples_chunked
from constants import DATA_JSONL_PATH, TABLE_NAME


def async_insert_into(glob_conn, executor, chunk_size: int, limit: int = -1):
    """
    params:
    connection: PG connector
    chunk_size: int, number of rows inserted to pg in single query
    limit: int, total number of rows to be insterted to pg
    """
    create_table(glob_conn, TABLE_NAME)
    data = get_data_as_tuples_chunked(
        DATA_JSONL_PATH, limit=limit, chunk_size=chunk_size
    )

    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    try:
        event_loop.run_until_complete(run_insert_into_loop(executor, data, glob_conn))
    finally:
        event_loop.close()


@async_measure
async def run_insert_into_loop(executor, data: list[list[tuple[Row]]], glob_conn):
    """
    params:
    data: list[list[tuple[Row]]], list of (author, text, likes, video_id) tuples
    """
    # pg_conn_pool = create_pg_connection_pool(executor._max_workers)
    # pg_conn_pool_size = len(pg_conn_pool)

    loop = asyncio.get_event_loop()
    tasks = [
        loop.create_task(insert_into(glob_conn, chunk))
        for idx_, chunk in enumerate(data)
    ]
    _ = await asyncio.gather(*tasks)
    # close_pg_connection_pool(pg_conn_pool)


def create_pg_connection_pool(size: int):
    def create_connection():
        conn = psycopg2.connect(
            host="localhost",
            port=os.environ["PG_PORT"],
            database="pgvector-test",
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
        )
        conn.autocommit = True
        if not conn.closed:
            return conn

    if size > 1:
        return [create_connection() for i in range(0, size - 1)]
    else:
        return [create_connection()]


def close_pg_connection_pool(pool):
    for p in pool:
        p.close()


if __name__ == "__main__":
    load_dotenv()
    glob_conn = psycopg2.connect(
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    glob_conn.autocommit = True

    if not glob_conn.closed:
        print("Connected to postgres\n")

    executor = ThreadPoolExecutor(max_workers=1)

    async_insert_into(glob_conn, executor, chunk_size=1, limit=-10)

    glob_conn.close()
