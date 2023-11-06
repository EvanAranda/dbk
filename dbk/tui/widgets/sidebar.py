import sqlalchemy as sa
from textual.widgets import Tree

from dbk.core import models

from ._util import get_session
from .nav import Navigator, RouteInfo
from .book import Book
from .transactions import Transactions


class NavTree(Navigator):
    DEFAULT_CSS = """
    NavTree > Tree {
        padding: 1 2;
    }
    """

    def compose(self):
        tree = Tree("root", id="nav-tree")
        tree.show_root = False
        books = tree.root.add("Books")
        self.books = books

        tree.root.add("Spending")
        tree.root.add("Portfolio")
        tree.root.add("Balance Sheet")
        tree.root.add("Transactions", Transactions.route_for())

        yield tree

    def action_reload(self) -> None:
        self.books.remove_children()
        with get_session(self.app) as sess:
            for book in sess.scalars(sa.select(models.Book)).all():
                self.books.add(book.name, Book.route_for(book.id))

    def on_mount(self) -> None:
        self.action_reload()

    def on_tree_node_selected(self, e: Tree.NodeSelected[RouteInfo]):
        if e.node.data is not None:
            self.post_message(self.Navigated(e.node.data))
