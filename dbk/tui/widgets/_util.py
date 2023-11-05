import sqlalchemy.orm as orm

from textual.app import App


def get_session_factory(app: App) -> orm.sessionmaker:
    from dbk.tui import MyApp

    assert isinstance(app, MyApp)
    return app._model.session_factory


def get_session(app: App) -> orm.Session:
    return get_session_factory(app)()
