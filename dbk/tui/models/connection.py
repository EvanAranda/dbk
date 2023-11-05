import logging
from pathlib import Path
from typing import Sequence

import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models, sync
from dbk.tui.settings import UserConfig

log = logging.getLogger(__name__)


class ConnectionModel:
    def __init__(
        self,
        session_factory: orm.sessionmaker[orm.Session],
        conn_id: int,
    ):
        self._session_factory = session_factory
        self.conn_id = conn_id

    def accounts(self):
        with self._session_factory() as s:
            s.expire_on_commit = False
            stmt = sa.select(models.Account).where(
                models.Account.conn_id == self.conn_id
            )
            return list(s.scalars(stmt).all())

    def data_sources(self):
        with self._session_factory() as s:
            s.expire_on_commit = False
            return list(
                s.scalars(
                    sa.select(models.DataSource)
                    .options(
                        orm.with_expression(
                            models.DataSource.num_transactions,
                            models.DataSource.num_transactions_expr(
                                models.Transaction.source_id
                            ),
                        )
                    )
                    .where(models.DataSource.conn_id == self.conn_id)
                ).all()
            )

    def add_file_data_sources(self, fnames: Sequence[str]):
        with self._session_factory() as s:
            s.expire_on_commit = False
            conn = s.get_one(models.Connection, self.conn_id)
            for fname in fnames:
                sync.create_file_data_source(s, conn, Path(fname))
