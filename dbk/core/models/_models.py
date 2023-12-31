import enum
from datetime import datetime
from typing import Any, Sequence

import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property


class _Base:
    @property
    def _session(self) -> orm.Session:
        s = orm.object_session(self)
        if s is None:
            raise RuntimeError("object not attached to session")
        return s


class Base(orm.DeclarativeBase, _Base):
    pass


class Book(Base):
    __tablename__ = "books"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(unique=True)

    connections: orm.Mapped[list["Connection"]] = orm.relationship(
        back_populates="book"
    )

    accounts: orm.Mapped[list["Account"]] = orm.relationship(back_populates="book")
    currency: orm.Mapped[str]

    @property
    def root_accounts(self) -> Sequence["Account"]:
        return self._session.scalars(
            sa.select(Account).where(
                Account.book_id == self.id,
                Account.is_root == True,
            )
        ).all()


class _ContainsTransactions(_Base):
    id: orm.Mapped[int]

    @declared_attr
    def num_transactions(self) -> orm.Mapped[int]:
        return orm.query_expression()  # type: ignore

    @classmethod
    def num_transactions_expr(cls, crit):
        return (
            sa.select(sa.func.count(Transaction.id))
            .where(crit == cls.id)
            .correlate_except(Transaction)
            .scalar_subquery()
        )


class Connection(Base, _ContainsTransactions):
    __tablename__ = "connections"
    __table_args__ = (
        sa.UniqueConstraint(
            "book_id",
            "provider_id",
            "conn_name",
            name="unique_conn_per_book",
        ),
    )
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    book_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Book.id, ondelete="cascade"),
    )
    provider_id: orm.Mapped[str]
    provider_data: orm.Mapped[dict[str, Any]] = orm.mapped_column(sa.JSON)
    conn_name: orm.Mapped[str]

    book: orm.Mapped[Book] = orm.relationship(back_populates="connections")
    accounts: orm.Mapped[list["Account"]] = orm.relationship(
        back_populates="connection"
    )


class DataSourceType(enum.StrEnum):
    file = "file"
    api = "api"


class DataSource(Base, _ContainsTransactions):
    __tablename__ = "data_sources"
    __table_args__ = (
        sa.UniqueConstraint(
            "conn_id",
            "name",
            "hash",
            name="unqiue_data_source_per_connection",
        ),
    )
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    conn_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Connection.id, ondelete="cascade"),
    )
    name: orm.Mapped[int]
    type: orm.Mapped[DataSourceType]
    hash: orm.Mapped[str | None]
    last_synced: orm.Mapped[datetime | None]
    last_sync_error: orm.Mapped[str | None]

    connection: orm.Mapped[Connection] = orm.relationship()


class AccountType(enum.StrEnum):
    asset = "asset"
    liability = "liability"
    income = "income"
    expense = "expense"


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (
        sa.UniqueConstraint(
            "book_id",
            "category",
            name="unique_category_per_book",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    book_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Book.id, ondelete="cascade"),
    )
    name: orm.Mapped[str]
    account_type: orm.Mapped[AccountType]
    is_root: orm.Mapped[bool]
    """Whether this account is a root account (i.e. not allowed to have a parent)"""
    is_virtual: orm.Mapped[bool]
    """Whether this account is a group of other accounts."""
    currency: orm.Mapped[str | None]
    """Currency of this account. Cannot be None if this account is not a group."""
    parent_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey("accounts.id", ondelete="cascade")
    )
    conn_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey(Connection.id, ondelete="cascade")
    )
    conn_label: orm.Mapped[str | None]
    """Label assigned by a connection provider to identify this account."""

    category: orm.Mapped[str | None]
    """
    Unique category that describes the purpose of this account. 
    Can be used by rules to automatically assign transactions to this account.
    """

    children: orm.Mapped[list["Account"]] = orm.relationship(back_populates="parent")
    parent: orm.Mapped["Account | None"] = orm.relationship(
        back_populates="children",
        remote_side=[id],
    )
    connection: orm.Mapped[Connection | None] = orm.relationship(
        back_populates="accounts"
    )
    book: orm.Mapped[Book] = orm.relationship(back_populates="accounts")

    @property
    def is_detached(self) -> bool:
        return self.parent_id is None and self.is_root == False


class TransactionType(enum.StrEnum):
    unknown = "unknown"
    transfer = "transfer"
    receive = "receive"
    spend = "spend"
    trade = "trade"


class Transaction(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        sa.UniqueConstraint(
            "conn_id",
            "time",
            "description",
            "credit_amount",
            "debit_amount",
            name="unique_transaction_per_connection",
            sqlite_on_conflict="IGNORE",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    book_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Book.id, ondelete="cascade"),
    )
    conn_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey(Connection.id, ondelete="cascade")
    )
    source_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey(DataSource.id, ondelete="cascade")
    )
    credit_account_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey(Account.id, ondelete="cascade"),
    )
    debit_account_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey(Account.id, ondelete="cascade"),
    )
    duplicate_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey("transactions.id")
    )
    type: orm.Mapped[TransactionType]
    time: orm.Mapped[datetime]
    description: orm.Mapped[str]
    user_description: orm.Mapped[str | None]
    credit_amount: orm.Mapped[float | None]
    debit_amount: orm.Mapped[float | None]
    external_ref: orm.Mapped[str | None]

    credit_account: orm.Mapped[Account | None] = orm.relationship(
        foreign_keys=[credit_account_id]
    )
    debit_account: orm.Mapped[Account | None] = orm.relationship(
        foreign_keys=[debit_account_id]
    )
