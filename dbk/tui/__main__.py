import asyncio
import os
import time

import sqlalchemy.orm as orm

from dbk import core, db, background
from dbk.logging import setup_logging
from dbk.tui import MyApp, MyAppModel
from dbk.tui.settings import RootConfig, UserConfig

root_config = RootConfig()  # type: ignore
user_config = UserConfig()

user_config.working_dir.mkdir(parents=True, exist_ok=True)
os.chdir(user_config.working_dir)

setup_logging()

engine = db.make_connection(root_config.db_url)
session = orm.sessionmaker(bind=engine)

core.initialize(session)

with background.WorkerPool(3) as pool:
    model = MyAppModel(session, pool)
    app = MyApp(model)
    app.run()
