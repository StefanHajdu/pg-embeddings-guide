import time
import psycopg2.extras
import io
from functools import wraps

from constants import TABLE_NAME, TIME_LOG_PATH
from my_types import Row


def measure(func):
    @wraps(func)
    def measure_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(
            f"{func.__name__} {kwargs.get("chunk_size", "")} -> {1000*(end - start)} ms"
        )
        with open(TIME_LOG_PATH, "a") as f:
            f.write(
                f"{func.__name__} {kwargs.get("chunk_size", "")} -> {1000*(end - start)} ms\n"
            )
        return result

    return measure_wrapper


def create_table(connection, table_name: str = TABLE_NAME):
    with connection.cursor() as cursor:
        query = """
            DROP TABLE IF EXISTS {table_name} CASCADE;
            CREATE UNLOGGED TABLE {table_name} (
                video_id            CHARACTER(255),
                author              TEXT,
                text                TEXT,
                likes               INTEGER
            );
        """
        cursor.execute(query.format_map({"table_name": table_name}))


def insert_into(connection, chunk: list[tuple[Row]]):
    with connection.cursor() as cursor:
        query = f"""
            INSERT INTO {TABLE_NAME} (author, text, likes, video_id)
            VALUES %s
        """
        psycopg2.extras.execute_values(cursor, query, chunk)


def prepare_insert_into(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
                PREPARE insert_plan (text, text, int, text) AS
                INSERT INTO {TABLE_NAME} (author, text, likes, video_id) VALUES($1, $2, $3, $4)
            """
        )


def copy_from(connection, chunk: io.TextIOBase):
    chunk.seek(io.SEEK_SET)
    with connection.cursor() as cursor:
        cursor.copy_from(
            chunk, TABLE_NAME, sep="\t", columns=("author", "text", "likes", "video_id")
        )
