import sqlalchemy.orm as orm
from textual.widget import Widget


class AccountModel:
    def __init__(self, session_factory: orm.sessionmaker[orm.Session], account_id: int):
        self._session_factory = session_factory
        self._account_id = account_id
