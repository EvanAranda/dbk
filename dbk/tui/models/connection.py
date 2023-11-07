import logging
from pathlib import Path
from typing import Sequence

import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models, persist, sync

log = logging.getLogger(__name__)


class ConnectionModel:
    def __init__(
        self,
        session_factory: orm.sessionmaker[orm.Session],
        storage: persist.Storage,
        conn_id: int,
    ):
        self._session_factory = session_factory
        self._storage = storage
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
        with self._session_factory() as session:
            session.expire_on_commit = False
            conn = session.get_one(models.Connection, self.conn_id)
            for fname in fnames:
                sync.create_file_data_source(
                    session,
                    self._storage,
                    conn,
                    Path(fname),
                )
