import sqlalchemy.orm as orm

from dbk.core import models
from . import _dsl as dsl


class RulesEngine:
    def __init__(self, session: orm.Session, rulesets: list[dsl.RuleSet]):
        self._session = session
        self._rulesets = rulesets

    def apply_rules(self, tx: models.Transaction):
        pass
