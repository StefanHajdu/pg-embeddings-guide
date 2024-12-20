import asyncio as aio
from ollama import AsyncClient
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from typing import TypedDict


class DbItem(TypedDict):
    idx: str
    text: int


OLLAMA_PORT = 11_434


class OllamaSimplePool:
    def __init__(self):
        self.client = AsyncClient(host=f"http://localhost:{OLLAMA_PORT}")

    def embed_bulk(self, bulk: list[DbItem]):
        return aio.run(self.send_bulk(bulk))

    async def send_bulk(self, bulk: list[DbItem]):
        responses = await aio.gather(
            *[self._embed_coro(self.client, item.get("text", "")) for item in bulk]
        )
        return responses

    def _embed_coro(self, ollama_aclient: AsyncClient, text: str):
        return ollama_aclient.embeddings(
            model="nomic-embed-text",
            prompt=text,
        )


if __name__ == "__main__":
    load_dotenv()

    host = ("localhost",)
    port = (os.environ["PG_PORT"],)
    database = ("pgvector-test",)
    user = (os.environ["PG_USER"],)
    password = (os.environ["PG_PASSWORD"],)
    connection_str = (
        f"postgresql://{user[0]}:{password[0]}@{host[0]}:{port[0]}/{database[0]}"
    )

    ollamaPool = OllamaSimplePool()
    engine = create_engine(connection_str)
    with engine.connect() as conn:
        for df in pd.read_sql(
            "SELECT idx, text FROM youtube_comments", conn, chunksize=10
        ):
            bulk = df.to_dict(orient="records")
            responses = ollamaPool.embed_bulk(bulk=bulk)

            for b, r in zip(bulk, responses):
                print(b["idx"], b["text"][:10], r.embedding[:5])
                print()
            break
