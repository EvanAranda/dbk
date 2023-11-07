from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, TextIO

import sqlalchemy.orm as orm
from pydantic import BaseModel
from dbk.core import persist

if TYPE_CHECKING:
    from dbk.core import models


@dataclass
class SyncContext[T: BaseModel]:
    session: orm.Session
    storage: persist.Storage
    provider: "Provider[T]"
    connection: "models.Connection"
    data_source: Optional["models.DataSource"] = None

    @property
    def provider_data(self) -> T:
        return self.provider.custom_data_model().model_validate(
            self.connection.provider_data
        )

    def read_data_source(self) -> TextIO:
        assert self.data_source is not None, "data_source must be set"
        return self.storage.read_stream(self.data_source)


class Provider[T: BaseModel](ABC):
    @classmethod
    @abstractmethod
    def provider_id(cls) -> str:
        ...

    @classmethod
    @abstractmethod
    def provider_name(cls) -> str:
        ...

    @classmethod
    @abstractmethod
    def custom_data_model(cls) -> type[T]:
        ...

    @abstractmethod
    def sync(self, context: SyncContext):
        ...
