from pathlib import Path
from typing import TextIO

from dbk import errors
from dbk.core import models
from dbk.tui.settings import UserConfig


class DataSourceStorage:
    def __init__(self, config: UserConfig | None = None):
        self.config = config or UserConfig()

    def get_data_source_path(self, ds: models.DataSource) -> Path:
        assert ds.type == models.DataSourceType.file
        assert ds.id is not None
        assert ds.conn_id is not None

        wd = self.config.working_dir
        conn_d = wd / "user_data" / "connections" / str(ds.conn_id)
        conn_d.mkdir(parents=True, exist_ok=True)
        return conn_d / str(ds.id)

    def open_data_source_file(self, ds: models.DataSource) -> TextIO:
        if ds.type != models.DataSourceType.file:
            raise errors.DbkError(f"Expected file data source, got {ds.type}")
        return open(self.get_data_source_path(ds), "r")
