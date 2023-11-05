import click

from dbk.core import models
from dbk.db import migrate

from ._app import App


@click.group()
def database():
    pass


@database.command()
@click.pass_obj
def reset(app: App):
    print("Resetting database...")
    migrate(app.engine, models.Base.metadata)
    print("Done!")
