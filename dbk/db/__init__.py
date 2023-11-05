import os

import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models


def make_connection(db_url: str) -> sa.Engine:
    return sa.create_engine(db_url)


def migrate(conn: sa.Engine, *metadatas: sa.MetaData):
    for metadata in metadatas:
        metadata.drop_all(conn)
        metadata.create_all(conn)
