import logging

import sqlalchemy as sa
import sqlalchemy.orm as orm
from pydantic import BaseModel

from dbk.background import WorkerPool, jobs
from dbk.core import models, persist, providers, rules

from .account import AccountModel
from .connection import ConnectionModel
from .modals import CreateAccountArgs, CreateConnectionArgs

log = logging.getLogger(__name__)


class BookModel:
    def __init__(
        self,
        book_id: int,
        session_factory: orm.sessionmaker[orm.Session],
        background_workers: WorkerPool,
        storage: persist.Storage,
        rules_engine: rules.RulesEngine,
    ):
        self._session_factory = session_factory
        self._workers = background_workers
        self._storage = storage
        self._rules_engine = rules_engine
        self.book_id = book_id

    def account_model(self, account_id: int):
        return AccountModel(self._session_factory, account_id)

    def connection_model(self, conn_id: int):
        return ConnectionModel(self._session_factory, self._storage, conn_id)

    def book(self):
        with self._session_factory() as sess:
            return sess.get_one(models.Book, self.book_id)

    def connections(self):
        with self._session_factory() as s:
            s.expire_on_commit = False
            stmt = sa.select(models.Connection).where(
                models.Connection.book_id == self.book_id
            )
            return s.scalars(stmt).all()

    def root_nodes(self):
        with self._session_factory() as s:
            s.expire_on_commit = False
            stmt = sa.select(models.Account).where(
                models.Account.book_id == self.book_id,
                models.Account.is_root == True,
            )
            return list(s.scalars(stmt).all())

    def child_nodes(self, parent: models.Account) -> list[models.Account]:
        with self._session_factory() as s:
            s.expire_on_commit = False
            return list(
                s.scalars(
                    sa.select(models.Account).where(
                        models.Account.parent_id == parent.id
                    )
                ).all()
            )

    def create_connection(self, args: CreateConnectionArgs):
        with self._session_factory() as s, s.begin():
            s.expire_on_commit = False
            provider = providers.find_provider(args.provider_id)
            model_cls: type[BaseModel] = provider.custom_data_model()
            model_cls.model_validate(args.provider_data)
            conn = models.Connection(
                book_id=self.book_id,
                conn_name=args.name,
                provider_id=args.provider_id,
                provider_data=args.provider_data,
            )
            s.add(conn)

        log.info("created connection %s", conn.conn_name)

    def sync_connection(self, conn: models.Connection):
        return self._workers.submit(jobs.sync_data_sources(conn.id))

    def apply_rules(self):
        return self._workers.submit(jobs.apply_rules(self.book_id))

    def create_account(self, args: CreateAccountArgs):
        with self._session_factory() as s, s.begin():
            s.expire_on_commit = False
            parent = s.get_one(models.Account, args.parent_id)
            account = models.Account(
                book_id=self.book_id,
                parent_id=args.parent_id,
                name=args.name,
                account_type=parent.account_type,
                is_root=False,
                is_virtual=args.create_group,
                currency=parent.book.currency if not args.create_group else None,
            )
            s.add(account)

        log.info(f"created account {account.name} under {parent.name}")

    def move_account(self, source: models.Account, target: models.Account):
        with self._session_factory() as s, s.begin():
            s.expire_on_commit = False
            s.add(source)
            s.add(target)
            assert source.id != target.id, "Cannot move an account under itself."
            assert (
                source.account_type == target.account_type
            ), f"{source.name} can only be moved under another '{source.account_type}' account."
            assert target.is_virtual, f"{target.name} is not a group."
            assert source.is_root == False, "Cannot move root accounts."
            source.parent_id = target.id

        log.info(f"moved account '{source.name}' under '{target.name}'")

    def delete_account(self, account: models.Account):
        with self._session_factory() as s, s.begin():
            s.delete(account)
            name = account.name

        log.info(f"deleted account {name}")
