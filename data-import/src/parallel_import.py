from functools import partial
import os
import psycopg2
from dotenv import load_dotenv
import time

import asyncio
from concurrent.futures import ThreadPoolExecutor

import psycopg2.pool

from utils import async_measure
from db_utils import (
    create_table,
    insert_into_pool,
    async_pg_select_sleep,
)
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
    pg_aconn_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=executor._max_workers,
        maxconn=executor._max_workers,
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )

    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    s = time.perf_counter()
    try:
        event_loop.run_until_complete(
            run_insert_into_loop(executor, pg_aconn_pool, data)
        )
    finally:
        event_loop.close()
        print(time.perf_counter() - s)
        pg_aconn_pool.closeall()


# @async_measure
async def run_insert_into_loop(executor, pg_aconn_pool, data: list[list[tuple[Row]]]):
    """
    params:
    data: list[list[tuple[Row]]], list of (author, text, likes, video_id) tuples
    """
    loop = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(executor, insert_into_pool, pg_aconn_pool, chunk)
        for chunk in data
    ]
    _ = await asyncio.gather(*tasks)


def async_sleep(executor, num_sleeps=1):
    pg_aconn_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=executor._max_workers,
        maxconn=executor._max_workers,
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    try:
        event_loop.run_until_complete(
            run_sleep_loop(executor, pg_aconn_pool, num_sleeps)
        )
    finally:
        event_loop.close()
        pg_aconn_pool.closeall()


@async_measure
async def run_sleep_loop(executor, pg_aconn_pool, num_sleeps):
    loop = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(executor, async_pg_select_sleep, pg_aconn_pool)
        for i in range(num_sleeps)
    ]
    res = await asyncio.gather(*tasks)
    print(res)


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

    executor = ThreadPoolExecutor(max_workers=10)
    async_insert_into(glob_conn, executor, chunk_size=1000, limit=-1)

    glob_conn.close()
