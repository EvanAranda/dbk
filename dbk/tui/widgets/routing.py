import logging
from abc import ABC, abstractmethod
from typing import Callable

from textual.reactive import reactive
from textual.widget import Widget

from .nav import RouteInfo

RouteHandler = Callable[[RouteInfo], Widget | None]

log = logging.getLogger(__name__)


class Routable:
    def update_route(self, route: RouteInfo) -> None:
        pass


class Router(Widget, Routable):
    route: reactive[RouteInfo | None] = reactive(None)

    def __init__(self, route_handler: RouteHandler, **kwargs):
        self._route_handler = route_handler
        super().__init__(**kwargs)

    async def watch_route(self, route: RouteInfo | None):
        await self.remove_children()
        if route is None:
            log.debug("no route")
            return
        if (content := self._route_handler(route)) is None:
            log.debug("no content")
            return

        await self.mount(content)
        self.children[0].focus()
        log.debug("mounted view %s", type(content).__name__)

    def update_route(self, route: RouteInfo) -> None:
        self.route = route
