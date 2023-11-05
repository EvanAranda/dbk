import logging

import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models

log = logging.getLogger(__name__)


class TransactionsModel:
    def __init__(self, session_factory: orm.sessionmaker[orm.Session]):
        self.session_factory = session_factory
        self.limit = 100
        self.offset = 0
        self.sort_order = sa.desc(models.Transaction.time)

    def transactions(self):
        with self.session_factory() as s:
            s.expire_on_commit = False
            stmt = (
                sa.select(models.Transaction)
                .offset(self.offset)
                .limit(self.limit)
                .order_by(self.sort_order)
            )

            # ensure the credit_account and debit_account relationships are loaded
            stmt = stmt.options(
                orm.joinedload(models.Transaction.credit_account),
                orm.joinedload(models.Transaction.debit_account),
            )

            return list(s.scalars(stmt).all())
