import os
import socket
import json
import sys
import psycopg2 as pg2
import argparse

from dotenv import load_dotenv

from sentence_transformers import SentenceTransformer

parser = argparse.ArgumentParser()
parser.add_argument("socket", type=str)
args = parser.parse_args()

if not args.socket:
    sys.exit(-1)

SOCK_FILE = args.socket
local_embed_model = SentenceTransformer(
    "/home/stephenx/LLMs/ollama/third-party/safetensors/nomic-embed-text-v1.5",
    trust_remote_code=True,
)


def update_pg_with_embed(pg_conn, row_json: dict[str:str], embed: list[float]):
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """
                UPDATE youtube_comments SET embed = %s WHERE idx = %s
            """,
            (embed, row_json.get("idx")),
        )
        pg_conn.commit()
    except Exception as e:
        print(f"in update_pg_with_embed: {e}")


def prepare_server_socket():
    try:
        os.unlink(SOCK_FILE)
    except:
        if os.path.exists(SOCK_FILE):
            raise

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    try:
        s.bind(SOCK_FILE)
        s.listen(1)
        print("Server socket set up!")
        return s
    except Exception as e:
        print(e)
        sys.exit(-1)


s = prepare_server_socket()
load_dotenv()
pg_conn = pg2.connect(
    host="localhost",
    port=os.environ["PG_PORT"],
    database="pgvector-test",
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
)
pg_conn.autocommit = True

if not pg_conn.closed:
    print("Postgres connected")


while True:
    socket_conn, addr = s.accept()
    print("Connection by client")

    buffer = ""
    while True:
        data = socket_conn.recv(4096)
        if not data:
            break
        else:
            data_json_str = data.decode("utf-8")
            buffer += data_json_str

    try:
        data_json = json.loads(buffer)
        embed = local_embed_model.encode(data_json.get("text", ""))
        update_pg_with_embed(pg_conn, data_json, embed.tolist())
    except Exception as e:
        print(f"in connection loop: {e}")
    finally:
        print("processed\n")