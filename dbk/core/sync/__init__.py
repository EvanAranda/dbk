import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Sequence

import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models, persist, providers, rules

log = logging.getLogger(__name__)


def blob_checksum(blob: BinaryIO) -> str:
    return hashlib.sha256(blob.read()).hexdigest()


def tx_checksum(conn_id: int, time: datetime, desc: str, amount: float) -> str:
    msg = f"{conn_id}:{time.isoformat()}:{desc}:{abs(amount)}".encode("utf-8")
    return hashlib.md5(msg).hexdigest()


def find_data_sources(
    session: orm.Session,
    conn_id: int,
    where="unsynced",
):
    stmt = sa.select(models.DataSource).where(models.DataSource.conn_id == conn_id)

    match where:
        case "unsynced":
            stmt = stmt.where(
                sa.or_(
                    models.DataSource.last_synced == None,
                    models.DataSource.last_sync_error != None,
                ),
            )

    return session.scalars(stmt).all()


def sync_connection(
    session: orm.Session,
    storage: persist.Storage,
    connection: models.Connection,
    sources: list[models.DataSource],
):
    provider = providers.find_provider(connection.provider_id)
    ctx = providers.SyncContext(session, storage, provider, connection)

    if not sources:
        provider.sync(ctx)
        return

    log.debug("found %s data sources to sync", len(sources))

    for source in sources:
        log.debug("syncing data source %s", source.name)
        try:
            with session.begin_nested():
                ctx.data_source = source
                provider.sync(ctx)
        except Exception as e:
            source.last_sync_error = str(e)
        finally:
            source.last_synced = datetime.now()


def create_file_data_source(
    session: orm.Session,
    storage: persist.Storage,
    connection: models.Connection,
    fname: Path,
) -> models.DataSource:
    session.expire_on_commit = False

    fname = fname.absolute()

    with open(fname, "rb") as f:
        fhash = blob_checksum(f)

    ds = session.scalar(
        sa.select(models.DataSource).where(
            models.DataSource.conn_id == connection.id,
            models.DataSource.hash == fhash,
        )
    )

    if ds is None:
        ds = models.DataSource(
            conn_id=connection.id,
            name=fname.name,
            hash=fhash,
            type=models.DataSourceType.file,
        )

        session.add(ds)
        session.commit()

        log.info(
            "added file data source %s from '%s' to connection %s",
            fname.name,
            str(fname),
            str(connection.id),
        )

    try:
        with open(fname, "r") as f:
            storage.write_stream(ds, f)

        # storage.write_stream()
        # ds_fname = storage.get_data_source_path(ds)
        # shutil.copyfile(fname, ds_fname)
        log.info("copied file '%s' to storage.", str(fname))
    except Exception as e:
        log.error("failed to copy file '%s'", str(fname), exc_info=e)
        ds.last_sync_error = "failed to copy file"
        session.commit()

    return ds


def apply_rules(txs: Sequence[models.Transaction], scope: rules.Scope):
    pass
