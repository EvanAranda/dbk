from abc import ABC, abstractmethod
from pathlib import Path
from typing import TextIO, override

from dbk import errors
from dbk.core import models
from dbk.settings import UserConfig


class Storage(ABC):
    @abstractmethod
    def write_stream(self, ds: models.DataSource, stream: TextIO):
        """Write the contents of the given stream to the storage for the data source."""

    @abstractmethod
    def read_stream(self, ds: models.DataSource) -> TextIO:
        """Read the contents of the given data source from the storage."""


class LocalStorage(Storage):
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

    @override
    def write_stream(self, ds: models.DataSource, stream: TextIO):
        if ds.type != models.DataSourceType.file:
            raise errors.DbkError(f"Expected file data source, got {ds.type}")
        with open(self.get_data_source_path(ds), "w") as f:
            f.write(stream.read())

    @override
    def read_stream(self, ds: models.DataSource) -> TextIO:
        if ds.type != models.DataSourceType.file:
            raise errors.DbkError(f"Expected file data source, got {ds.type}")
        return open(self.get_data_source_path(ds), "r")
