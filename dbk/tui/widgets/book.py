import logging
from dataclasses import dataclass
from datetime import datetime

from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import (
    Button,
    DataTable,
    Label,
    Static,
    TabbedContent,
    TabPane,
    Tree,
)
from textual.widgets.data_table import RowKey
from textual.widgets.tree import TreeNode

from dbk.core import models
from dbk.tui.error_handling import Message, use_error_handler

from ..models.book import BookModel
from .account import Account
from .connection import Connection
from .modals import (
    CreateAccountArgs,
    CreateConnectionArgs,
    NewAccountModal,
    NewConnectionModal,
)
from .nav import Navigatable, Navigator, RouteInfo
from .routing import Routable, Router

log = logging.getLogger(__name__)


@dataclass
class ConnectionItem:
    connection: models.Connection
    syncing = False


class ConnectionsList(Navigator):
    BINDINGS = [
        ("o", "goto_connection", "Goto Connection"),
    ]

    selected: reactive[ConnectionItem | None] = reactive(None)
    connections = reactive([])

    def __init__(self, model: BookModel, *args, **kwargs):
        self._model = model
        self._table = DataTable()
        self._table.add_column("Name")
        self._table.add_column("Last Synced")
        self._rows: dict[RowKey, ConnectionItem] = {}
        super().__init__(*args, **kwargs)

    def compose(self):
        with Horizontal():
            with Vertical(classes="buttons"):
                yield Button("Sync All", id="sync-all")
            yield self._table

    def on_mount(self):
        self.action_load_connections()

    def on_data_table_cell_selected(self, e: DataTable.CellSelected):
        e.stop()
        row = e.cell_key.row_key
        if row in self._rows:
            self.selected = self._rows[row]

    def on_data_table_cell_highlighted(self, e: DataTable.CellHighlighted):
        e.stop()
        row = e.cell_key.row_key
        if row in self._rows:
            self.selected = self._rows[row]

    @use_error_handler
    async def watch_connections(self, conns: list[models.Connection]):
        try:
            self.loading = True

            self._table.clear()
            self._rows.clear()
            self.selected = None

            for conn in conns:
                ci = ConnectionItem(conn)
                row = self._table.add_row(
                    conn.conn_name,
                    "",  # TODO: add last synced
                    key=str(conn.id),
                )
                self._rows[row] = ci
        except:
            raise
        finally:
            self.loading = False

    def action_load_connections(self):
        self.connections = self._model.connections()

    async def sync_connection(self, conn: models.Connection):
        # conn_item = self.query_one(f"#conn-{conn.id}", ConnectionItem)
        # conn_item.syncing = True
        try:
            self.app.notify(f"Syncing {conn.conn_name}...", severity="information")
            await self._model.sync_connection(conn)
            self.app.notify(f"Synced {conn.conn_name}", severity="information")

            self.app.notify(f"Apply rules...", severity="information")
            await self._model.apply_rules()
            self.app.notify(f"Finished rules", severity="information")
        except Exception as e:
            log.exception("sync failed")
            self.app.notify(f"Failed to sync {conn.conn_name}", severity="error")
        finally:
            pass
            # conn_item.syncing = False

    def action_goto_connection(self):
        if self.selected is None:
            return
        self.navigate_to_route(Connection.route_for(self.selected.connection.id))

    def _conn_id(self, conn: models.Connection) -> str:
        return f"conn-{conn.id}"


class AccountTree(Navigator):
    BINDINGS = [
        ("a", "create_account(False)", "Create Account"),
        ("g", "create_account(True)", "Create Group"),
        ("x", "cut_account", "Cut Account"),
        ("v", "move_account", "Paste Account"),
        ("d", "delete_account", "Delete Account"),
        ("o", "goto_account", "Goto Account"),
    ]

    DEFAULT_CSS = """
    Tree {
        background: $surface;
    }
    """

    buffer: reactive[TreeNode[models.Account] | None] = reactive(None)
    selected: reactive[TreeNode[models.Account] | None] = reactive(None)
    root_nodes: reactive[list[models.Account]] = reactive([])

    def __init__(self, model: BookModel, *args, **kwargs):
        self._model = model
        self._accounts_tree = Tree("accounts", id="account-tree")
        self._accounts_tree.show_root = False
        super().__init__(*args, **kwargs)

    def compose(self):
        yield self._accounts_tree

    def on_show(self):
        self.action_load()

    def on_tree_node_selected(self, e: Tree.NodeSelected):
        self.selected = e.node

    def on_tree_node_highlighted(self, e: Tree.NodeHighlighted):
        self.selected = e.node

    def on_tree_node_expanded(self, e: Tree.NodeExpanded):
        self._reload_children(e.node)

    def watch_root_nodes(self, nodes: list[models.Account]):
        tree = self._accounts_tree
        tree.clear()

        for n in nodes:
            nn = tree.root.add(n.name, n)
            nn.expand()

    @use_error_handler
    def action_load(self):
        self.root_nodes = self._model.root_nodes()

    @use_error_handler
    def action_create_account(self, create_group: bool):
        n = self.selected
        if n is None:
            return

        account = n.data
        if account is None or not account.is_virtual:
            raise Message.inform("Select a group to create an account under.")

        @use_error_handler(app=self.app)
        def callback(args: CreateAccountArgs | None):
            if not args:
                return
            self._model.create_account(args)
            self._reload_children(n)

        self.app.push_screen(NewAccountModal(account.id, create_group), callback)

    @use_error_handler
    def action_move_account(self):
        """
        Moves the account in `self.buffer` under the account in `self.selected`.
        """
        if self.buffer is None or self.selected is None:
            self.buffer = None
            return

        if self.buffer.data is None or self.selected.data is None:
            return

        try:
            self._model.move_account(self.buffer.data, self.selected.data)

            # reload affected parts of tree
            if self.buffer.parent:
                self._reload_children(self.buffer.parent)
            self._reload_children(self.selected)
        except:
            raise
        finally:
            self.buffer = None

    def action_cut_account(self):
        self.buffer = self.selected

    @use_error_handler
    def action_delete_account(self):
        if self.selected is None or self.selected.data is None:
            return

        if self.selected.data.is_root:
            raise Message.warn("Cannot delete root accounts.")

        # TODO: confirm with user
        self._model.delete_account(self.selected.data)
        self._accounts_tree.select_node(self.selected.parent)
        self.selected.remove()
        self.selected = None

    def action_goto_account(self):
        if self.selected is None or self.selected.data is None:
            return
        self.navigate_to_route(Account.route_for(self.selected.data.id))

    def _reload_children(self, node: TreeNode[models.Account]):
        account = node.data
        if not isinstance(account, models.Account):
            return

        node.remove_children()
        children = self._model.child_nodes(account)
        for child in children:
            if child.is_virtual:
                node.add(child.name, child)
            else:
                node.add_leaf(child.name, child)


class Book(Navigator, Navigatable, Routable):
    BINDINGS = [
        ("c", "create_connection", "Create Connection"),
        ("s", "sync_connection", "Sync Connection"),
    ]

    route_name = "book"
    book = reactive(None)

    def __init__(self, model: BookModel, *args, **kwargs):
        self._model = model
        self._connections_list = ConnectionsList(self._model)
        self._accounts_tree = AccountTree(self._model)
        self._router = Router(self._route_handler)
        super().__init__(*args, **kwargs)

    def compose(self):
        yield Static("", id="book-title")
        with Vertical():
            with Vertical():
                with TabbedContent():
                    with TabPane("Accounts Tree", id="book-accounts"):
                        with Vertical():
                            yield self._accounts_tree
                    with TabPane("Connections", id="book-connections"):
                        with Vertical():
                            yield self._connections_list
                    with TabPane("Rule Sets", id="book-rule-sets"):
                        with Vertical():
                            yield Label("rule sets")

            with Vertical():
                yield self._router

    def on_mount(self):
        self.action_load_book()

    def on_navigator_navigated(self, e: Navigator.Navigated):
        e.stop()
        self.update_route(e.route)

    def update_route(self, route: RouteInfo) -> None:
        self._router.route = route

    def _route_handler(self, route: RouteInfo):
        match route.path:
            case [Connection.route_name, int(conn_id), *_]:
                c = Connection(self._model.connection_model(conn_id))
                c.update_route(route[2:])
                return c
            case [Account.route_name, int(account_id), *_]:
                c = Account(self._model.account_model(account_id))
                return c

    def action_load_book(self):
        self.book = self._model.book()

    def watch_book(self, book: models.Book | None):
        if book:
            self.query_one("#book-title", Static).update(f"Book {book.name}")

    def action_create_connection(self):
        def callback(args: CreateConnectionArgs | None):
            if not args:
                return
            self._model.create_connection(args)
            self.app.call_later(self._connections_list.action_load_connections)

        self.app.push_screen(NewConnectionModal(), callback)

    def action_sync_connection(self):
        item = self._connections_list.selected
        if item is None:
            return

        # if syncing: return
        async def sync_worker():
            await self._connections_list.sync_connection(item.connection)
            # TODO: reload accounts tree
            # self._accounts_tree.action_load()

        self.run_worker(sync_worker(), exclusive=True)
