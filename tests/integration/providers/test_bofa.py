from unittest import mock

import io
import pytest
import sqlalchemy.orm as orm
import sqlalchemy as sa

from dbk.core import models, persist
from dbk.core.providers import SyncContext
from dbk.core.providers.bofa import BofaAccountType, BofaData, BofaProvider
from dbk.db import make_connection, make_session_factory, migrate

csv_input = """blank
blank
blank
blank
blank

Date,Description,Amount,Running Bal.
12/20/2022,0,,"26,012.56"
12/22/2022,1,"2,258.18","28,270.74"
12/27/2022,2,"1,460.51","29,731.25"
12/27/2022,3,-923.07,"28,808.18"
12/28/2022,4,"-2,152.44","26,655.74"
01/03/2023,5,42.28,"26,698.02"
01/03/2023,6,"-2,894.34","23,803.68"
01/05/2023,7,"2,277.69","26,081.37"
01/09/2023,8,-200.00,"25,881.37"
01/09/2023,9,-78.32,"25,803.05"
01/19/2023,10,"2,277.69","28,080.74"
01/19/2023,11,-15.99,"28,064.75"
"""


@pytest.fixture
def session():
    e = make_connection("sqlite:///:memory:")
    migrate(e, models.Base.metadata)
    sf = make_session_factory(e)
    with sf() as s:
        s.expire_on_commit = False
        yield s


def test_sync_without_sources(session: orm.Session):
    with session.begin_nested():
        book = models.Book(name="test", currency="USD")
        conn = models.Connection(
            book=book,
            provider_id=BofaProvider.provider_id(),
            conn_name="test",
            provider_data=BofaData(account_type=BofaAccountType.checking).model_dump(),
        )
        session.add(book)
        session.add(conn)

    storage = mock.MagicMock(spec=persist.Storage)

    # def read_stream(ds):
    #     assert
    # storage.read_stream.side_effect = lambda ds: io.StringIO(csv_input)

    provider = BofaProvider()
    ctx = SyncContext(
        session=session,
        storage=storage,
        provider=provider,
        connection=conn,
    )
    provider.sync(ctx)

    assert len(conn.accounts) == 1

    account = conn.accounts[0]
    assert account.name == "test"
    assert account.account_type == models.AccountType.asset
    assert account.conn_label == "checking"


def test_sync_with_source(session: orm.Session):
    with session.begin_nested():
        book = models.Book(name="test", currency="USD")
        conn = models.Connection(
            book=book,
            provider_id=BofaProvider.provider_id(),
            conn_name="test",
            provider_data=BofaData(account_type=BofaAccountType.checking).model_dump(),
        )
        source = models.DataSource(
            name="test",
            type=models.DataSourceType.file,
            connection=conn,
        )

        session.add(book)
        session.add(conn)
        session.add(source)

    storage = mock.MagicMock(spec=persist.Storage)

    def read_stream(ds):
        assert ds is source
        return io.StringIO(csv_input)

    storage.read_stream.side_effect = read_stream

    provider = BofaProvider()
    ctx = SyncContext(
        session=session,
        storage=storage,
        provider=provider,
        connection=conn,
        data_source=source,
    )

    provider.sync(ctx)

    assert len(conn.accounts) == 1
    assert (
        session.scalar(sa.select(sa.func.count()).select_from(models.Transaction)) == 11
    )
