import click
import sqlalchemy as sa

from dbk.core import models
from ._app import App


@click.group("book")
def subcommand():
    pass


def make_commands(model_cls: type, group):
    @group.command()
    @click.pass_obj
    def list_models(app: App):
        with app.session_factory() as s:
            models = s.scalars(sa.select(model_cls)).all()
            for model in models:
                print(model)


make_commands(models.Book, subcommand)
