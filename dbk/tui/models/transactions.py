from dataclasses import dataclass
import logging
from typing import Callable

import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models, rules

log = logging.getLogger(__name__)


@dataclass
class Pagination:
    limit: int
    offset: int
    total: int

    @property
    def num_pages(self) -> int:
        return self.total // self.limit

    @property
    def current_page(self) -> int:
        return self.offset // self.limit

    @property
    def offset_end(self) -> int:
        return min(self.offset + self.limit, self.total)

    def goto_page(self, page: int):
        page = min(page, self.num_pages)
        page = max(page, 0)
        self.offset = page * self.limit


class TransactionsModel:
    def __init__(
        self,
        session_factory: orm.sessionmaker[orm.Session],
        rules_loader: Callable[[], rules.Scope],
    ):
        self.session_factory = session_factory
        self.pagination = Pagination(limit=100, offset=0, total=0)

        self.sort_order = sa.desc(models.Transaction.time)
        self.filter_uncategorized = False

    def transactions(self):
        with self.session_factory() as s:
            s.expire_on_commit = False
            stmt = sa.select(
                models.Transaction,
                sa.func.count().over(),
            )

            # ensure the credit_account and debit_account relationships are loaded
            stmt = stmt.options(
                orm.joinedload(models.Transaction.credit_account),
                orm.joinedload(models.Transaction.debit_account),
            )

            if self.filter_uncategorized:
                stmt = stmt.where(
                    sa.or_(
                        models.Transaction.credit_account_id == None,
                        models.Transaction.debit_account_id == None,
                    )
                )

            stmt = (
                stmt.order_by(self.sort_order)
                .offset(self.pagination.offset)
                .limit(self.pagination.limit)
            )

            results = s.execute(stmt).all()

            if not results:
                self.pagination.total = 0
                return []
            else:
                self.pagination.total = results[0][1]
                return [r[0] for r in results]

    def run_rules(self):
        raise NotImplementedError()
