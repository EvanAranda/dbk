from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import sqlalchemy.orm as orm
from pydantic import BaseModel

if TYPE_CHECKING:
    from dbk.core import models


@dataclass
class SyncContext[T: BaseModel]:
    session: orm.Session
    provider: "Provider[T]"
    connection: "models.Connection"
    data_source: Optional["models.DataSource"] = None

    @property
    def provider_data(self) -> T:
        return self.provider.custom_data_model().model_validate(
            self.connection.provider_data
        )


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
