import logging
from datetime import datetime

import sqlalchemy as sa

from dbk.core import models, persist, rules, sync
from dbk.db import make_connection, make_session_factory
from dbk.settings import RootConfig

from .worker_pool import Job


class _worker_context:
    def __init__(self):
        self.config = RootConfig()  # type: ignore
        self.engine = make_connection(self.config.db_url)
        self.session_factory = make_session_factory(self.engine)
        self.storage = persist.LocalStorage()


def _sync_data_sources_job(conn_id):
    ctx = _worker_context()
    log = logging.getLogger(__name__)

    with ctx.session_factory() as session:
        session.expire_on_commit = False
        conn = session.get_one(models.Connection, conn_id)

        log.debug("begin sync of connection %s", conn.conn_name)

        try:
            sources = list(sync.find_data_sources(session, conn.id))
            sync.sync_connection(session, ctx.storage, conn, sources)
            session.commit()

            log.info("synced connection %s", conn.conn_name)

            return len(sources)
        except Exception as e:
            log.error("sync of connection %s failed", conn.conn_name, exc_info=e)
            raise


def _apply_rules(book_id: int, rulesets: list[rules.RuleSet]):
    ctx = _worker_context()
    log = logging.getLogger(__name__)

    with ctx.session_factory() as session:
        stmt = sa.select(models.Transaction).where(
            models.Transaction.book_id == book_id,
            sa.or_(
                models.Transaction.credit_account_id == None,
                models.Transaction.debit_account_id == None,
            ),
        )

        txs = session.scalars(stmt).all()
        engine = rules.RulesEngine(session, rulesets)
        for tx in txs:
            engine.apply_rules(tx)


def sync_data_sources(conn_id: int) -> Job[int]:
    """
    Syncs all unsynced data sources for the given connection.

    :return: Job that will complete with the number of data sources synced
    """
    return Job(_sync_data_sources_job, conn_id)


def apply_rules(book_id: int, rulesets: list[rules.RuleSet]):
    return Job(_apply_rules, book_id, rulesets)
