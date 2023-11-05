import sqlalchemy as sa
import sqlalchemy.orm as orm

from dbk.core import models
from dbk.errors import DbkError


class InitializationFailed(DbkError):
    pass


def initialize(session: orm.sessionmaker[orm.Session]):
    """
    Ensures there is at least one book in the database. If there is not, a book is
    created with the name "My Book" and the currency "USD". Four root accounts are
    added to the book: "Assets", "Liabilities", "Incomes", and "Expenses".

    :raises InitializationFailed: if this method fails in any way
    """
    try:
        with session() as s, s.begin():
            if has_any_books(s):
                return
            add_default_book(s)
    except Exception as e:
        raise InitializationFailed("Failed to initialize database") from e


def has_any_books(session: orm.Session) -> bool:
    return session.execute(sa.select(sa.func.count(models.Book.id))).scalar_one() > 0


def add_default_book(session: orm.Session) -> models.Book:
    book = models.Book(name="My Book", currency="USD")
    session.add(book)

    root_accounts = [
        ("Assets", models.AccountType.asset),
        ("Liabilities", models.AccountType.liability),
        ("Incomes", models.AccountType.income),
        ("Expenses", models.AccountType.expense),
    ]

    for name, account_type in root_accounts:
        book.accounts.append(
            models.Account(
                name=name,
                account_type=account_type,
                is_root=True,
                is_virtual=True,
            )
        )

    return book
