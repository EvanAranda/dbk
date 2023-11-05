import logging

from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Button, DataTable, Label, TabbedContent, TabPane

from dbk.core import models

from ..models.connection import ConnectionModel
from .nav import Navigatable, Navigator, RouteInfo
from .routing import Routable

log = logging.getLogger(__name__)


class AccountsList(Navigator):
    accounts: reactive[list[models.Account]] = reactive([])

    def __init__(self, model: ConnectionModel, **kwargs):
        self._model = model
        self._table = DataTable()
        self._table.add_columns("Name", "Type", "Currency")
        super().__init__(**kwargs)

    def compose(self):
        yield self._table

    def on_show(self):
        self.action_load_accounts()

    def action_load_accounts(self):
        self.accounts = self._model.accounts()

    def watch_accounts(self, accounts: list[models.Account]):
        self._table.clear()

        for account in accounts:
            self._table.add_row(
                account.name,
                account.account_type.value,
                account.currency,
            )


class DataSourcesList(Navigator):
    sources: reactive[list[models.DataSource]] = reactive([])

    def __init__(self, model: ConnectionModel, **kwargs):
        self._model = model
        self._table = DataTable()
        self._table.add_columns("Name", "Type", "Last Used", "#Transactions")
        self._table.add_row("test", "test", "test", "test")
        super().__init__(**kwargs)

    def compose(self):
        with Horizontal():
            with Vertical(classes="buttons"):
                yield Button("Add File", id="add-file")
                yield Button("Add API", id="add-api")
            yield self._table

    def on_show(self):
        self.action_load_sources()

    def on_button_pressed(self, e: Button.Pressed):
        match e.button.id:
            case "add-file":
                e.stop()
                self.action_add_file()

    def action_load_sources(self):
        self.sources = self._model.data_sources()

    def action_add_file(self):
        try:
            from tkinter.filedialog import askopenfilenames

            fnames = askopenfilenames()
            self._model.add_file_data_sources(fnames)
        except Exception as e:
            self.app.notify("Unable to add file(s).", severity="error")

    def watch_sources(self, sources: list[models.DataSource]):
        self._table.clear()

        for source in sources:
            self._table.add_row(
                source.name,
                source.type,
                source.last_synced,
                source.num_transactions,
            )


class Connection(Widget, Navigatable, Routable):
    route_name = "connection"

    def __init__(self, model: ConnectionModel, *args, **kwargs):
        self._model = model
        self._data_sources = DataSourcesList(self._model)
        self._accounts_list = AccountsList(self._model)

        super().__init__(*args, **kwargs)

    def compose(self):
        yield Label("Connection")

        with TabbedContent():
            with TabPane("Accounts", id="connection-accounts"):
                with Vertical():
                    yield self._accounts_list
            with TabPane("Data Sources", id="connection-data-sources"):
                with Vertical():
                    yield self._data_sources
            with TabPane("Settings", id="connection-settings"):
                with Vertical():
                    yield Label("settings")

    def update_route(self, route: RouteInfo) -> None:
        match route.query:
            case {"connection_tab": str(tab_id)}:
                self.get_child_by_type(TabbedContent).show_tab(tab_id)
