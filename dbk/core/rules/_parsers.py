import functools as ft

from parsy import *  # type: ignore

from ._dsl import (
    FieldTest,
    Operators,
    ReferencedRuleSet,
    ReferencedTest,
    ReferencedAction,
    SetField,
    UseRuleSet,
    Identifier,
    Literal,
)

space = regex(r"\s+")
scope_sep = string("::")
ident = regex(r"[\w_]+")
scoped_ident = ident.sep_by(scope_sep, min=2)
reference = (scoped_ident | (scope_sep >> ident).times(1)).map(Identifier)

tx_field_name = (
    string("desc") | string("description") | string("amount") | string("time")
)

_ops_map = {
    "is": Operators.equals,
    "is not": Operators.not_equals,
    "contains": Operators.contains,
    "does not contain": Operators.not_contains,
}
op = ft.reduce(
    lambda a, b: a | b,
    [string(_op_name).result(_op) for _op_name, _op in _ops_map.items()],
)

value = regex(r"[^,]+").map(Literal)

field_test = seq(
    tx_field_name << space,
    op,
    space >> value,
).combine(FieldTest)

referenced_test = reference.map(ReferencedTest)

test_parser = referenced_test | field_test

kw_set = string("set")
kw_to = string("to")
kw_use = string("use")

set_field_action = seq(
    kw_set >> space >> tx_field_name << space,
    kw_to >> space >> value,
).combine(SetField)

use_ruleset_action = (
    kw_use >> space >> ident.times(1).map(Identifier).map(ReferencedRuleSet)
).map(UseRuleSet)

referenced_action = reference.map(ReferencedAction)

action_parser = set_field_action | use_ruleset_action | referenced_action
