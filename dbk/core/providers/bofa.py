import csv
import enum
from datetime import datetime
from typing import Any, Iterable, TextIO, override

import sqlalchemy as sa
import sqlalchemy.dialects.sqlite as sa_sqlite
from pydantic import BaseModel

from dbk.core import models, persist, sync

from ._providers import Provider, SyncContext


class BofaAccountType(enum.StrEnum):
    checking = "checking"
    savings = "savings"
    credit = "credit"

    def to_account_type(self) -> models.AccountType:
        match self:
            case BofaAccountType.checking | BofaAccountType.savings:
                return models.AccountType.asset
            case BofaAccountType.credit:
                return models.AccountType.liability


class BofaData(BaseModel):
    account_type: BofaAccountType


class BofaProvider(Provider[BofaData]):
    def __init__(self):
        pass

    @classmethod
    @override
    def provider_id(cls) -> str:
        return "bofa"

    @classmethod
    @override
    def provider_name(cls) -> str:
        return "Bank of America"

    @classmethod
    @override
    def custom_data_model(cls):
        return BofaData

    @override
    def sync(self, context: SyncContext[BofaData]):
        context.session.expire_on_commit = False

        conn = context.connection
        provider = context.provider
        provider_data = context.provider_data
        bofa_account_type = provider_data.account_type

        # ==============================
        # Make Account
        # ==============================
        accounts = {a.conn_label: a for a in conn.accounts}
        conn_label = bofa_account_type.value
        if (account := accounts.get(conn_label)) is None:
            account = models.Account(
                name=conn.conn_name,
                account_type=bofa_account_type.to_account_type(),
                is_root=False,
                is_virtual=False,
                currency=conn.book.currency,
                conn_id=conn.id,
                conn_label=conn_label,
                book_id=conn.book_id,
            )
            conn.accounts.append(account)

            for ra in conn.book.root_accounts:
                if ra.account_type == account.account_type:
                    ra.children.append(account)
                    break

            context.session.commit()

        # ==============================
        # Make Transactions
        # ==============================
        if not (source := context.data_source):
            return

        if source.type != models.DataSourceType.file:
            return

        reader_factory = make_reader(bofa_account_type)
        with context.storage.read_stream(source) as f:
            txs = list(parse_txs(conn, source, account, reader_factory(f)))

        stmt = sa_sqlite.insert(models.Transaction).values(txs)
        context.session.execute(stmt)
        context.session.commit()


def parse_txs(conn, source, account, txs: Iterable[dict]):
    for _tx in txs:
        credit_account_id, debit_account_id = None, None
        credit_amount, debit_amount = None, None

        if _tx["amount"] < 0:
            credit_account_id = account.id
            credit_amount = abs(_tx["amount"])
        else:
            debit_account_id = account.id
            debit_amount = abs(_tx["amount"])

        yield dict(
            book_id=conn.book_id,
            conn_id=conn.id,
            source_id=source.id,
            time=_tx["time"],
            type=models.TransactionType.unknown,
            description=_tx["description"],
            credit_account_id=credit_account_id,
            debit_account_id=debit_account_id,
            credit_amount=credit_amount,
            debit_amount=debit_amount,
        )


def make_reader(account: BofaAccountType):
    skip_lines = 0

    match account:
        case BofaAccountType.checking:
            skip_lines = 8
            fields = [
                ("time", 0, _parse_time),
                ("description", 1, _noop),
                ("amount", 2, _parse_amount),
            ]
        case BofaAccountType.credit:
            raise NotImplementedError()
        case BofaAccountType.savings:
            raise NotImplementedError()

    def reader(f: TextIO) -> Iterable[dict[str, Any]]:
        for _ in range(skip_lines):
            f.readline()

        rdr = csv.reader(f)
        for row in rdr:
            yield {k: parse(row[i]) for k, i, parse in fields}

    return reader


def _noop(x: str) -> str:
    return x


def _parse_amount(x: str) -> float:
    return float(x.replace("$", "").replace(",", ""))


def _parse_time(x: str) -> datetime:
    return datetime.strptime(x, "%m/%d/%Y")
