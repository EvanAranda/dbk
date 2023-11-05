import logging

from rich.text import Text
from textual.reactive import reactive
from textual.widgets import DataTable

from dbk.core import models

from ..models.transactions import TransactionsModel
from .nav import Navigatable, Navigator

log = logging.getLogger(__name__)


class Transactions(Navigator, Navigatable):
    route_name = "txs"
    default_opts = {"limit": 100, "offset": 0}
    search_opts: reactive[dict] = reactive(
        dict(default_opts), always_update=True, init=False
    )

    columns = (
        {"label": "Date"},
        {"label": "Description", "width": 60},
        {"label": "Type"},
        {"label": "Credited"},
        {"label": "Debited"},
        {"label": "Credits"},
        {"label": "Debits"},
    )
    txs: reactive[list[models.Transaction]] = reactive([])

    def __init__(self, model: TransactionsModel, **kwargs):
        self._model = model
        self._set_search_opts(self.search_opts)
        self._table = DataTable(zebra_stripes=True)
        for col in self.columns:
            self._table.add_column(**col)  # type: ignore
        super().__init__(**kwargs)

    def compose(self):
        yield self._table

    def on_mount(self):
        self.action_reload()

    def watch_txs(self, txs: list[models.Transaction]):
        self._table.clear()
        for tx in txs:
            self._table.add_row(
                tx.time.strftime("%Y-%m-%d"),
                Text(tx.description),
                tx.type,
                tx.credit_account.name if tx.credit_account else "-",
                tx.debit_account.name if tx.debit_account else "-",
                Text.from_markup(
                    f"[bright_black]{tx.credit_account.currency}[/] {tx.credit_amount}"
                    if tx.credit_account
                    else "-",
                    justify="right",
                ),
                Text.from_markup(
                    f"[bright_black]{tx.debit_account.currency}[/] {tx.debit_amount}"
                    if tx.debit_account
                    else "-",
                    justify="right",
                ),
                key=str(tx.id),
                height=3,
            )

    def watch_search_opts(self, opts: dict):
        self._set_search_opts(opts)
        self.action_reload()

    def action_reload(self):
        self.txs = self._model.transactions()
        log.info(f"reloaded {len(self.txs)} transactions {self.search_opts!r}")

    def _set_search_opts(self, opts: dict):
        self._model.offset = opts["offset"]
        self._model.limit = opts["limit"]
