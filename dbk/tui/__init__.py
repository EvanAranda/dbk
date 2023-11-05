import logging

import sqlalchemy.orm as orm
from textual.app import App
from textual.widgets import Footer, Header

from dbk.background import WorkerPool
from dbk.tui.widgets.book import Book, BookModel
from dbk.tui.widgets.nav import Navigator, RouteInfo
from dbk.tui.widgets.routing import Routable, Router
from dbk.tui.widgets.sidebar import NavTree
from dbk.tui.widgets.transactions import Transactions, TransactionsModel

log = logging.getLogger(__name__)


class MyAppModel:
    def __init__(
        self,
        session_factory: orm.sessionmaker,
        background_workers: WorkerPool,
    ):
        self.session_factory = session_factory
        self.background_workers = background_workers

    def book_model(self, book_id: int):
        return BookModel(book_id, self.session_factory, self.background_workers)

    def transactions_model(self):
        return TransactionsModel(self.session_factory)


class MyApp(App, Routable):
    CSS_PATH = "styles.tcss"

    def __init__(self, model: MyAppModel):
        self._model = model
        self._navtree = NavTree(id="sidebar")
        self._router = Router(self._route_handler, id="main")
        super().__init__()

    def compose(self):
        yield Header()
        yield self._navtree
        yield self._router
        yield Footer()

    def on_navigator_navigated(self, message: Navigator.Navigated):
        self.title = " ".join(map(str, message.route.path))
        self.update_route(message.route)
        log.info("navigator navigated: %s", repr(message.route))

    def update_route(self, route: RouteInfo) -> None:
        self._router.update_route(route)

    def _route_handler(self, route: RouteInfo):
        match route.path:
            case [Book.route_name, int(book_id), *_]:
                content = Book(self._model.book_model(book_id))
                # content.update_route(route[2:])
                return content
            case [Transactions.route_name]:
                return Transactions(self._model.transactions_model())
