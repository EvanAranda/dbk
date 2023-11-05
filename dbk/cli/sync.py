from pathlib import Path

import click
import sqlalchemy as sa

from dbk.core import models, sync

from ._app import App


@click.command()
@click.argument("fname", type=Path)
@click.option("--conn", type=str, required=True)
@click.option("--book", type=str)
@click.pass_obj
def add_file(app: App, fname: Path, book: str | None, conn: str):
    with app.session_factory() as s:
        s.expire_on_commit = False

        stmt = sa.select(models.Connection)
        if book:
            stmt = stmt.join(models.Book).where(models.Book.name == book)
        stmt = stmt.where(models.Connection.conn_name == conn)
        conns = s.scalars(
            sa.select(models.Connection).where(models.Connection.conn_name == conn)
        ).all()

        if len(conn) > 1:
            raise ValueError(
                f"Connection {conn} is ambiguous - multiple connections with that name exist. Specify a book name to narrow the search."
            )

        connection = conns[0]
        data_source = sync.create_file_data_source(s, connection, fname)
        sync.sync_data_source(s, connection, data_source)
        s.commit()


@click.command()
@click.option("--book", type=str)
@click.option("--source", type=str, required=True)
@click.pass_obj
def resync(app: App, book: str | None, conn: str):
    with app.session_factory() as s:
        s.expire_on_commit = False

        stmt = sa.select(models.DataSource)
        if book:
            stmt = stmt.join(models.Book).where(models.Book.name == book)
        stmt = stmt.where(models.Connection.conn_name == conn)
        conns = s.scalars(
            sa.select(models.Connection).where(models.Connection.conn_name == conn)
        ).all()
