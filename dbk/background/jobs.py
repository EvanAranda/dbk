import logging
from datetime import datetime

from dbk.core import models, sync
from dbk.db import make_connection, make_session_factory
from dbk.tui.settings import RootConfig

from .worker_pool import Job


class _worker_context:
    def __init__(self):
        self.config = RootConfig()  # type: ignore
        self.engine = make_connection(self.config.db_url)
        self.session_factory = make_session_factory(self.engine)


def _sync_data_sources_job(conn_id):
    ctx = _worker_context()
    log = logging.getLogger(__name__)

    with ctx.session_factory() as s:
        s.expire_on_commit = False
        # s.add(conn)  # re-attach
        conn = s.get_one(models.Connection, conn_id)

        log.debug("begin sync of connection %s", conn.conn_name)

        try:
            sources = sync.find_data_sources(s, conn.id)

            if not sources:
                sync.sync_connection(s, conn)
            else:
                log.debug("found %s data sources to sync", len(sources))

                for source in sources:
                    with s.begin_nested():
                        sync.sync_data_source(s, conn, source)

            s.commit()

            log.info("synced connection %s", conn.conn_name)
            return len(sources)
        except Exception as e:
            log.error("sync of connection %s failed", conn.conn_name, exc_info=e)
            raise


def sync_data_sources(conn_id: int) -> Job[int]:
    """
    Syncs all unsynced data sources for the given connection.

    :return: Job that will complete with the number of data sources synced
    """
    return Job(_sync_data_sources_job, conn_id)


# def apply_rules()
