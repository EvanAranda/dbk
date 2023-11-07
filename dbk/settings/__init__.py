from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class RootConfig(BaseSettings):
    db_url: str


class UserConfig(BaseSettings):
    working_dir: Path = Path.home() / ".dbk"
