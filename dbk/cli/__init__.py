import click

from dbk.cli import database, sync, book
from dbk.core import initialize

from ._app import App


@click.group()
@click.pass_context
def main(ctx: click.Context):
    app = App()
    initialize(app.session_factory)
    ctx.obj = app


main.add_command(database.database)
main.add_command(sync.add_file)
main.add_command(book.subcommand)
