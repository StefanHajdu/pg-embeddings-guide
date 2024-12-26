import psycopg2.extras
import io
import os
import select

from constants import TABLE_NAME
from my_types import Row


def create_aconn():
    keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 5,
    }
    aconn = psycopg2.connect(
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        async_=1,
        **keepalive_kwargs,
    )
    if not aconn.closed:
        return aconn


def create_connection():
    aconn = psycopg2.connect(
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    if not aconn.closed:
        return aconn


def create_pg_aconn_pool(size: int):
    if size > 0:
        return [create_aconn() for _ in range(size)]
    else:
        return [create_aconn()]


def close_pg_aconn_pool(pool):
    for p in pool:
        p.close()


def create_table(connection, table_name: str = TABLE_NAME):
    with connection.cursor() as cursor:
        query = """
            DROP TABLE IF EXISTS {table_name} CASCADE;
            CREATE UNLOGGED TABLE {table_name} (
                video_id            CHARACTER(255),
                author              TEXT,
                text                TEXT,
                likes               INTEGER,
                embed               vector(768)
            );
        """
        cursor.execute(query.format_map({"table_name": table_name}))


def wait(conn):
    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)


def async_pg_select_sleep(pg_aconn_pool):
    aconn = pg_aconn_pool.getconn()
    # wait(aconn)
    acurs = aconn.cursor()
    acurs.execute("SELECT pg_sleep(1); SELECT 42;")
    # wait(acurs.connection)

    try:
        res = acurs.fetchone()
        return res[0]
    except:
        return "no"
    finally:
        pg_aconn_pool.putconn(aconn)


def insert_into_pool(pg_aconn_pool, chunk: list[tuple[Row]], chunk_id: int):
    conn = pg_aconn_pool.getconn()
    curs = conn.cursor()
    query = f"""
        INSERT INTO {TABLE_NAME} (author, text, likes, video_id) VALUES (%s, %s, %s, %s)
    """
    try:
        curs.executemany(query, chunk)
        conn.commit()
        return f"done: {chunk_id}"
    except Exception as e:
        print(e)
    finally:
        pg_aconn_pool.putconn(conn)


def insert_into(connection, chunk: list[tuple[Row]], chunk_id: int):
    with connection.cursor() as curs:
        query = f"""
            INSERT INTO {TABLE_NAME} (author, text, likes, video_id) VALUES (%s, %s, %s, %s)
        """
        try:
            curs.executemany(query, chunk)
            return f"done: {chunk_id}"
        except Exception as e:
            print(e)


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
