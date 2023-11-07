import os

import sqlalchemy.orm as orm

from dbk import background, core, db
from dbk.core import persist
from dbk.logging import setup_logging
from dbk.settings import RootConfig, UserConfig
from dbk.tui import MyApp, MyAppModel

root_config = RootConfig()  # type: ignore
user_config = UserConfig()

user_config.working_dir.mkdir(parents=True, exist_ok=True)
os.chdir(user_config.working_dir)

setup_logging("dbk.tui.log")

engine = db.make_connection(root_config.db_url)
session = orm.sessionmaker(bind=engine)
storage = persist.LocalStorage(user_config)

core.initialize(session)

with background.WorkerPool(3) as pool:
    model = MyAppModel(session, pool, storage)
    app = MyApp(model)
    app.run()
