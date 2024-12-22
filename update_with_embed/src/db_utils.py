from sqlalchemy import create_engine
import os


def open_sqlalchemy_conn():
    host = ("localhost",)
    port = (os.environ["PG_PORT"],)
    database = ("pgvector-test",)
    user = (os.environ["PG_USER"],)
    password = (os.environ["PG_PASSWORD"],)
    connection_str = (
        f"postgresql://{user[0]}:{password[0]}@{host[0]}:{port[0]}/{database[0]}"
    )
    return create_engine(connection_str)
