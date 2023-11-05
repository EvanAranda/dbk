from typing import ClassVar

from textual.message import Message
from textual.widget import Widget

PathComponent = str | int


class RouteInfo:
    def __init__(self, *path: PathComponent, **query: str):
        self.path: list[PathComponent] = list(path)
        self.query = query

    def __repr__(self) -> str:
        return f"RouteInfo({self.path!r}, {self.query!r})"

    def __getitem__(self, key):
        return RouteInfo(*self.path[key], **self.query)


class Navigatable:
    route_name: ClassVar[str]

    @classmethod
    def route_for(cls, *args, **kwargs) -> RouteInfo:
        return RouteInfo(cls.route_name, *args, **kwargs)


class Navigator(Widget):
    class Navigated(Message):
        def __init__(self, route: RouteInfo):
            self.route = route
            super().__init__()

    def navigate(self, *path, **query: str):
        return self.post_message(self.Navigated(RouteInfo(*path, **query)))

    def navigate_to(self, target: type[Navigatable], *args, **query: str):
        return self.navigate(target.route_name, *args, **query)

    def navigate_to_route(self, route: RouteInfo):
        return self.post_message(self.Navigated(route))
