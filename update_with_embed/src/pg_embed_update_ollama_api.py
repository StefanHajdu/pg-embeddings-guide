import asyncio as aio
import asyncpg as apg
import psycopg2 as pg2
import pandas as pd
import os
import time

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


class OllamaSimplePool:
    def __init__(self):
        self.client = AsyncClient(host=f"http://localhost:{OLLAMA_PORT}")

    @async_measure
    async def embed_bulk(self, bulk: list[DbItem]):
        responses = await aio.gather(
            *[self._embed_coro(self.client, item.get("text", "")) for item in bulk]
        )
        return responses

    def _embed_coro(self, ollama_aclient: AsyncClient, text: str):
        return ollama_aclient.embeddings(
            model="nomic-embed-text",
            prompt=text,
        )


@async_measure
async def update_pg_with_embed(
    apg_conn_pool, bulk: dict[str:str], responses: list[float]
):
    updated_data = [
        (str(response.embedding), str(bulk.get("idx")))
        for bulk, response in zip(bulk, responses)
    ]

    apg_conn = await apg_conn_pool.acquire()

    try:
        await apg_conn.executemany(
            """
                UPDATE youtube_comments SET embed = $1 WHERE idx = $2
            """,
            updated_data,
        )
    except Exception as e:
        print(e)
    finally:
        await apg_conn_pool.release(apg_conn)


@measure
def seq_update_pg_with_embed(pg_conn, bulk: dict[str:str], responses: list[float]):
    updated_data = [
        (str(response.embedding), str(bulk.get("idx")))
        for bulk, response in zip(bulk, responses)
    ]
    cur = pg_conn.cursor()
    cur.executemany(
        """
            UPDATE youtube_comments SET embed = %s WHERE idx = %s
        """,
        updated_data,
    )
    pg_conn.commit()


@async_measure
async def iterate_and_update(iterator_conn, ollamaPool, limit=1):
    pg_conn = pg2.connect(
        host="localhost",
        port=os.environ["PG_PORT"],
        database="pgvector-test",
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )

    cnt = 0
    with iterator_conn.connect() as conn:
        for df in pd.read_sql(
            "SELECT idx, text FROM youtube_comments", conn, chunksize=100
        ):
            s1 = time.perf_counter()
            if cnt == limit:
                break
            else:
                cnt += 1
                bulk = df.to_dict(orient="records")
                responses = await ollamaPool.embed_bulk(bulk=bulk)
                seq_update_pg_with_embed(pg_conn, bulk, responses)
                print(f"iteration: {cnt} in {time.perf_counter() - s1}")

    conn.close()


if __name__ == "__main__":
    load_dotenv()
    iterator_conn = open_sqlalchemy_conn()
    ollamaPool = OllamaSimplePool()
    aio.run(iterate_and_update(iterator_conn, ollamaPool))
