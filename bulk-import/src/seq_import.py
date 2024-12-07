import os
import io
import psycopg2
from dotenv import load_dotenv

from db_utils import (
    measure,
    create_table,
    insert_into,
    copy_from,
    prepare_insert_into,
)
from my_types import Row
from data_utils import get_data_as_tuples, get_data_as_csv
from constants import DATA_JSONL_PATH, TABLE_NAME


def seq_insert_into(connection, chunk_size: int, limit: int = -1):
    """
    params:
    connection: PG connector
    chunk_size: int, number of rows inserted to pg in single query
    limit: int, total number of rows to be insterted to pg
    """
    create_table(connection, TABLE_NAME)
    data = get_data_as_tuples(DATA_JSONL_PATH, limit=limit)
    run_insert_into_loop(connection, data, chunk_size=chunk_size)


def seq_insert_into_prepared(connection, chunk_size: int, limit: int = -1):
    """
    params:
    connection: PG connector
    chunk_size: int, number of rows inserted to pg in single query
    limit: int, total number of rows to be insterted to pg
    """
    create_table(connection, TABLE_NAME)
    prepare_insert_into(connection)
    data = get_data_as_tuples(DATA_JSONL_PATH, limit=limit)
    run_insert_into_loop(connection, data, chunk_size=chunk_size)


@measure
def run_insert_into_loop(connection, data: list[tuple[Row]], chunk_size: int):
    """
    params:
    connection: PG connector
    data: tuple[Row], list of (author, text, likes, video_id) tuples
    chunk_size: int, number of rows inserted to PG in single query
    """
    if chunk_size < 0:
        insert_into(connection, data)
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        insert_into(connection, chunk)


def seq_copy_from(connection, chunk_size: int, limit: int = -1):
    """
    params:
    connection: PG connector
    chunk_size: int, number of rows inserted to pg in single query
    limit: int, total number of rows to be insterted to pg
    """
    create_table(connection, TABLE_NAME)
    data = get_data_as_csv(DATA_JSONL_PATH, limit=limit)
    run_copy_from_loop(connection, data, chunk_size=chunk_size)


@measure
def run_copy_from_loop(connection, data: list[str], chunk_size: int):
    """
    params:
    connection: PG connector
    data: tuple[Row], list of (author, text, likes, video_id) tuples
    chunk_size: int, number of rows inserted to PG in single query
    """
    if chunk_size < 0:
        csv_ = io.StringIO()
        csv_.write("\n".join(data))
        copy_from(connection, csv_)
        csv_.close()
    else:
        for i in range(0, len(data), chunk_size):
            csv_ = io.StringIO()
            csv_.write("\n".join(data[i : i + chunk_size]))
            copy_from(connection, csv_)
            csv_.close()


if __name__ == "__main__":
    load_dotenv()
    conn = psycopg2.connect(
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    conn.autocommit = True

    if not conn.closed:
        print("Connected to postgres\n")

    # seq_insert_into(conn, chunk_size=-1)
    seq_insert_into(conn, chunk_size=1, limit=-1)
    # seq_insert_into(conn, chunk_size=5)
    # seq_insert_into(conn, chunk_size=10)

    # seq_insert_into_prepared(conn, chunk_size=1)
    # seq_insert_into_prepared(conn, chunk_size=5)
    # seq_insert_into_prepared(conn, chunk_size=10)

    # seq_copy_from(conn, chunk_size=-1)
    # seq_copy_from(conn, chunk_size=1)
    # seq_copy_from(conn, chunk_size=5)
    # seq_copy_from(conn, chunk_size=10)

    conn.close()
