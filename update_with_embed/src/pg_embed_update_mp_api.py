import asyncio as aio
import psycopg2 as pg2
import pandas as pd
import os
import time
import queue

from multiprocessing import JoinableQueue, Process
from ollama import AsyncClient
from dotenv import load_dotenv
from typing import TypedDict

from update_with_embed.src.utils.db_utils import open_sqlalchemy_conn
from update_with_embed.src.utils.measure_utils import async_measure, measure


class DbItem(TypedDict):
    idx: str
    text: str


class DbUpdatedItem(TypedDict):
    idx: str
    embed: list[float]


OLLAMA_PORT = 11_434
LIMIT = 100
CHUNKSIZE = 100


class OllamaSimplePool:
    def __init__(self):
        self.client = AsyncClient(host=f"http://localhost:{OLLAMA_PORT}")

    @async_measure
    async def embed_chunk(self, chunk: list[DbItem]):
        responses = await aio.gather(
            *[self._embed_coro(self.client, item.get("text", "")) for item in chunk]
        )
        return responses

    def _embed_coro(self, ollama_aclient: AsyncClient, text: str):
        return ollama_aclient.embeddings(
            model="nomic-embed-text",
            prompt=text,
        )


def pg_get_chunks_producer(q: JoinableQueue, pg_iterator_conn):
    cnt = 0
    with pg_iterator_conn.connect() as conn:
        for df in pd.read_sql(
            "SELECT idx, text FROM youtube_comments", conn, chunksize=CHUNKSIZE
        ):
            if cnt == LIMIT:
                break
            else:
                cnt += 1
                chunk = df.to_dict(orient="records")
                time.sleep(1)
                q.put(chunk)

    print("all fetched from db")
    q.join()


def pg_embed_update_consumer(q: JoinableQueue, ollama_pool, pg_conn):
    aio.run(main_consumer_loop(q, ollama_pool, pg_conn))


async def main_consumer_loop(q: JoinableQueue, ollama_pool, pg_conn):
    while True:
        try:
            chunk = q.get()
            responses = await ollama_pool.embed_chunk(chunk=chunk)
            seq_update_pg_with_embed(pg_conn, chunk, responses)
            q.task_done()
        except queue.Empty:
            print("q is empty")
            break


def seq_update_pg_with_embed(
    pg_conn, chunk: list[dict[str:str]], responses: list[float]
):
    updated_data = [
        (str(response.embedding), str(chunk.get("idx")))
        for chunk, response in zip(chunk, responses)
    ]
    cur = pg_conn.cursor()
    try:
        cur.executemany(
            """
                UPDATE youtube_comments SET embed = %s WHERE idx = %s
            """,
            updated_data,
        )
        pg_conn.commit()
    except Exception as e:
        print(e)
        pg_conn.rollback()


@measure
def main(q: JoinableQueue, ollama_pool, pg_conn, pg_iterator_conn):
    p_producer = Process(target=pg_get_chunks_producer, args=[q, pg_iterator_conn])
    p_consumer = Process(
        target=pg_embed_update_consumer, args=[q, ollama_pool, pg_conn], daemon=True
    )

    p_consumer.start()
    p_producer.start()

    p_producer.join()


if __name__ == "__main__":
    q = JoinableQueue()
    load_dotenv()
    ollama_pool = OllamaSimplePool()
    pg_conn = pg2.connect(
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    pg_iterator_conn = open_sqlalchemy_conn()

    main(q, ollama_pool, pg_conn, pg_iterator_conn)

    pg_conn.close()
    if pg_conn.closed:
        print("PG conn closed")
