import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import BinaryIO

import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models, persist, providers

log = logging.getLogger(__name__)


def blob_checksum(blob: BinaryIO) -> str:
    return hashlib.sha256(blob.read()).hexdigest()


def tx_checksum(conn_id: int, time: datetime, desc: str, amount: float) -> str:
    msg = f"{conn_id}:{time.isoformat()}:{desc}:{abs(amount)}".encode("utf-8")
    return hashlib.md5(msg).hexdigest()


def find_data_sources(session: orm.Session, conn_id: int, where="unsynced"):
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


def sync_data_source(
    session: orm.Session,
    connection: models.Connection,
    data_source: models.DataSource,
):
    try:
        provider = providers.find_provider(connection.provider_id)
        ctx = providers.SyncContext(session, provider, connection, data_source)
        provider.sync(ctx)
        data_source.last_sync_error = None
    except Exception as e:
        data_source.last_sync_error = str(e)
        raise
    finally:
        connection.last_synced = datetime.now()
        data_source.last_synced = datetime.now()


def create_file_data_source(
    session: orm.Session,
    connection: models.Connection,
    fname: Path,
    storage: persist.DataSourceStorage | None = None,
) -> models.DataSource:
    storage = storage or persist.DataSourceStorage()
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
        ds_fname = storage.get_data_source_path(ds)
        shutil.copyfile(fname, ds_fname)
        log.info("copied file '%s' to '%s'", str(fname), str(ds_fname))
    except Exception as e:
        log.error("failed to copy file '%s'", str(fname), exc_info=e)
        ds.last_sync_error = "failed to copy file"
        session.commit()

    return ds
