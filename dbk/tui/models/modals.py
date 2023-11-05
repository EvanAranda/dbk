from dataclasses import dataclass
from typing import Any


@dataclass
class CreateConnectionArgs:
    name: str
    provider_id: str
    provider_data: dict[str, Any]


@dataclass
class CreateAccountArgs:
    parent_id: int
    name: str
    create_group: bool
