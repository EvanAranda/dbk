from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, runtime_checkable

from dbk.core.models import Transaction

from ..models import Transaction


@dataclass
class Literal[T]:
    value: T


@dataclass
class Identifier:
    path: list[str]


@dataclass
class Reference[T]:
    ident: Identifier
    bound: T | None = None

    @property
    def value(self) -> T:
        if self.bound is None:
            raise ValueError(f"Reference {self.ident} is not bound.")
        return self.bound


type Value[T] = Literal[T] | Reference[T]


@runtime_checkable
class Visitable(Protocol):
    def __visit__(self, cb: Callable[[Any], None]) -> None:
        ...

    @classmethod
    def try_visit(cls, o, cb: Callable[[Any], None]) -> None:
        cb(o)
        if isinstance(o, Visitable):
            o.__visit__(cb)


class Action(Protocol):
    def __call__(self, tx: Transaction) -> None:
        ...


class Test(Protocol):
    def __call__(self, tx: Transaction) -> bool:
        ...


class Operator(Protocol):
    def __call__(self, a: Any, b: Any) -> bool:
        ...


class Operators:
    @staticmethod
    def contains(s: str, sub: str) -> bool:
        return sub in s

    @staticmethod
    def not_contains(s: str, sub: str) -> bool:
        return sub not in s

    @staticmethod
    def equals(a: Any, b: Any) -> bool:
        return a == b

    @staticmethod
    def not_equals(a: Any, b: Any) -> bool:
        return a != b

    @staticmethod
    def greater_than(a: Any, b: Any) -> bool:
        return a > b

    @staticmethod
    def less_than(a: Any, b: Any) -> bool:
        return a < b

    @staticmethod
    def greater_than_or_equal(a: Any, b: Any) -> bool:
        return a >= b

    @staticmethod
    def less_than_or_equal(a: Any, b: Any) -> bool:
        return a <= b


@dataclass
class ReferencedTest(Test, Reference[Test]):
    def __call__(self, tx: Transaction) -> bool:
        return self.value(tx)


@dataclass
class FieldTest(Test, Visitable):
    field: str
    operator: Operator
    operand: Value[Any]

    def __call__(self, tx: Transaction) -> bool:
        return self.operator(
            getattr(tx, self.field),
            self.operand.value,
        )

    def __visit__(self, cb: Callable[[Any], None]) -> None:
        cb(self.field)
        cb(self.operator)
        cb(self.operand)


@dataclass
class AndTest(Test, Visitable):
    tests: list[Test]

    def __call__(self, tx: Transaction) -> bool:
        for test in self.tests:
            if not test(tx):
                return False
        return True

    def __visit__(self, cb: Callable[[Any], None]) -> None:
        for t in self.tests:
            self.try_visit(t, cb)


@dataclass
class OrTest(Test, Visitable):
    tests: list[Test]

    def __call__(self, tx: Transaction) -> bool:
        for test in self.tests:
            if test(tx):
                return True
        return False

    def __visit__(self, cb: Callable[[Any], None]) -> None:
        for t in self.tests:
            self.try_visit(t, cb)


@dataclass
class NotTest(Test):
    test: Test

    def __call__(self, tx: Transaction) -> bool:
        return not self.test(tx)


@dataclass
class Rule(Visitable):
    test: Test
    then: Action

    def __call__(self, tx: Transaction) -> bool:
        if self.test(tx):
            self.then(tx)
            return True
        return False

    def __visit__(self, cb: Callable[[Any], None]) -> None:
        self.try_visit(self.test, cb)
        self.try_visit(self.then, cb)


@dataclass
class RuleSet(Visitable):
    name: str

    tests: dict[str, Test] = field(default_factory=dict)
    """Tests available in the scope of this ruleset. These can be referenced by rules."""

    actions: dict[str, Action] = field(default_factory=dict)
    """Actions available in the scope of this ruleset. These can be referenced by rules."""

    rules: dict[str, Rule] = field(default_factory=dict)
    """Rules that are applied in order until one succeeds."""

    dependencies: dict[str, Any] = field(default_factory=dict)
    """Other rulesets this depends on (has references to it's members.)"""

    def __call__(self, tx: Transaction) -> bool:
        for rule in self.rules.values():
            if rule(tx):
                return True
        return False

    def __visit__(self, cb: Callable[[Any], None]) -> None:
        for test in self.tests.values():
            self.try_visit(test, cb)

        for action in self.actions.values():
            self.try_visit(action, cb)

        for rule in self.rules.values():
            self.try_visit(rule, cb)


@dataclass
class ActionSequence(Action, Visitable):
    actions: list[Action]

    def __call__(self, tx: Transaction) -> None:
        for action in self.actions:
            action(tx)

    def __visit__(self, cb: Callable[[Any], None]) -> None:
        for action in self.actions:
            self.try_visit(action, cb)


@dataclass
class ReferencedRuleSet(Reference[RuleSet]):
    def __call__(self, tx: Transaction) -> bool:
        return self.value(tx)


@dataclass
class UseRuleSet(Action, Visitable):
    ruleset: ReferencedRuleSet

    def __call__(self, tx: Transaction) -> None:
        self.ruleset(tx)

    def __visit__(self, cb: Callable[[Any], None]) -> None:
        self.try_visit(self.ruleset, cb)


@dataclass
class ReferencedAction(Action, Reference[Action]):
    def __call__(self, tx: Transaction) -> None:
        self.value(tx)


@dataclass
class SetField(Action):
    field: str
    value: Value[Any]

    def __call__(self, tx: Transaction) -> None:
        setattr(tx, self.field, self.value.value)


@dataclass
class Scope:
    rulesets: dict[str, RuleSet] = field(default_factory=dict)
