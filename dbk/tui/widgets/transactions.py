import logging

from rich.text import Text
from textual.containers import Grid, Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import DataTable, Input, Label, Static

from dbk.core import models

from ..models.transactions import Pagination, TransactionsModel
from .nav import Navigatable, Navigator

log = logging.getLogger(__name__)


class SearchOptionsModal(ModalScreen):
    def __init__(self):
        pass


class Transactions(Navigator, Navigatable):
    DEFAULT_CSS = """
    #txs-topbar {
        padding: 1;
        height: auto;
    }

    #txs-topbar Label {
        padding: 0 2 0 0;
    }

    #txs-count {
        align-horizontal: right;
    }
    
    DataTable {
        height: 1fr;
    }
    """

    BINDINGS = [
        ("p", "prev_page", "Prev Page"),
        ("n", "next_page", "Next Page"),
        ("r", "reload", "Reload"),
    ]

    route_name = "txs"

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
    pagination: reactive[Pagination] = reactive(
        Pagination(limit=100, offset=0, total=0),
        always_update=True,
    )

    def __init__(self, model: TransactionsModel, **kwargs):
        self._model = model
        self._table = DataTable(zebra_stripes=True)
        for col in self.columns:
            self._table.add_column(**col)  # type: ignore
        super().__init__(**kwargs)

    def compose(self):
        with Vertical():
            with Horizontal(id="txs-topbar"):
                yield Label("[@click=prev_page()]Prev[/]")
                yield Label("[@click=next_page()]Next[/]")
                yield Label("Page _ of _", id="page-count")
                yield Static(
                    "Showing n txs, (x-y) of _",
                    id="txs-count",
                )
            yield self._table

    def on_mount(self):
        self.action_reload()

    def watch_txs(self, txs: list[models.Transaction]):
        self._table.clear()
        for tx in txs:
            self._table.add_row(
                tx.time.strftime("%Y-%m-%d"),
                Text(tx.description),
                Text.from_markup(f"[{_tx_type_color(tx.type)}]{tx.type}[/]"),
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

    def watch_pagination(self, p: Pagination):
        page_count = self.query_one("#page-count", Static)
        txs_count = self.query_one("#txs-count", Static)
        page_count.update(f"Page {p.current_page + 1} of {p.num_pages + 1}")
        txs_count.update(
            f"Showing {len(self.txs)} txs, ({p.offset}-{p.offset_end}) of {p.total}"
        )

    def action_reload(self):
        self.txs = self._model.transactions()
        self.pagination = self._model.pagination
        log.info(f"reloaded {len(self.txs)} transactions")

    def action_prev_page(self):
        self._model.pagination.goto_page(self._model.pagination.current_page - 1)
        self.action_reload()

    def action_next_page(self):
        self._model.pagination.goto_page(self._model.pagination.current_page + 1)
        self.action_reload()


def _tx_type_color(tx_type: models.TransactionType):
    match tx_type:
        case models.TransactionType.unknown:
            return "bright_black"
        case models.TransactionType.spend:
            return "red"
        case models.TransactionType.receive:
            return "green"
        case models.TransactionType.transfer:
            return "blue"
        case models.TransactionType.trade:
            return "yellow"
