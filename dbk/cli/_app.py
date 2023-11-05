import os
import sqlalchemy.orm as orm

from dbk.db import make_connection
from dbk.tui.settings import RootConfig, UserConfig


class App:
    def __init__(self):
        self.root_config = RootConfig()  # type: ignore
        self.user_config = UserConfig()

        self.user_config.working_dir.mkdir(parents=True, exist_ok=True)
        os.chdir(self.user_config.working_dir)

        self.engine = make_connection(self.root_config.db_url)
        self.session_factory: orm.sessionmaker[orm.Session] = orm.sessionmaker(
            self.engine
        )
