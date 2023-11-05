from textual.widget import Widget

from ..models.account import AccountModel
from .nav import Navigatable


class Account(Widget, Navigatable):
    route_name = "account"

    def __init__(self, model: AccountModel, **kwargs):
        self._model = model
        super().__init__(**kwargs)
