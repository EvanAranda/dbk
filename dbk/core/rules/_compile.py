from functools import partial
from typing import Any, Literal

import yaml

from . import _dsl as dsl
from ._dsl import ActionSequence, AndTest, NotTest, OrTest, Rule, RuleSet, Scope, Test
from ._parsers import action_parser, test_parser

RuleDoc = dict[str, Any]
RuleSetDoc = dict[str, RuleDoc]
RuleSetKey = Literal["tests", "actions", "rules"]


def compile_rules(yaml_str: str) -> Scope:
    doc = yaml.load(yaml_str, Loader=yaml.CLoader)
    scope = Scope({k: compile_rule_set(k, v) for k, v in doc.items()})
    return scope


def resolve_references(scope: Scope) -> None:
    def resolve(rs: RuleSet, key: RuleSetKey, i: list[str]):
        match i:
            # Refers to something in rs
            case [str(local_ident)]:
                return getattr(rs, key)[local_ident]
            # Refers to something in rs_ident
            case [str(rs_ident), *rest]:
                return resolve(scope.rulesets[rs_ident], key, rest)

    def visitor(rs: RuleSet, x):
        match x:
            case dsl.ReferencedRuleSet():
                x.bound = scope.rulesets[x.ident.path[0]]
            case dsl.ReferencedAction():
                x.bound = resolve(rs, "actions", x.ident.path)
            case dsl.ReferencedTest():
                x.bound = resolve(rs, "tests", x.ident.path)

    # Traverse each ruleset structure and apply the visitor to each node.
    # The visitor updates nodes which are reference types to bind them to
    # the value their identifier is pointing to.
    for rs in scope.rulesets.values():
        rs.__visit__(partial(visitor, rs))


def compile_rule_set(
    rule_set_name: str,
    rule_set_doc: RuleSetDoc,
) -> RuleSet:
    """
    Converts a yaml document tree into a RuleSet.

    Assumes the object has a structure like:

    ```yaml
    <rule_set_name>:
        <rule_set_args>
    ```

    Where `<rule_set_args>` one or more of:
    ```yaml
    tests:
        <test_args>
    actions:
        <action_args>
    rules:
        <rule_args>
    ```

    Where `<test_args>` is one or more of:
    ```yaml
    <test_name>: <test>
    <test_name>:
        <test>
    <test_name>:
        - <test>
    ```

    Where `<rule_args>` is:
    ```yaml
    - <rule_name>:
        test: <test_name> or <test>
        test:
            - <test_name> or <test>
        then: <action_name> or <action>
        then:
            - <action_name> or <action>
    ```
    """
    rule_set = RuleSet(name=rule_set_name)

    for k, v in rule_set_doc.get("tests", {}).items():
        rule_set.tests[k] = compile_test(v)

    for k, v in rule_set_doc.get("actions", {}).items():
        rule_set.actions[k] = compile_action(v)

    for k, v in rule_set_doc.get("rules", {}).items():
        rule_set.rules[k] = compile_rule(k, v)

    return rule_set


def compile_test(doc: Any) -> Test:
    if isinstance(doc, list):
        return AndTest([compile_test(d) for d in doc])
    elif isinstance(doc, dict):
        match doc:
            case {"and": [*tests]}:
                return AndTest([compile_test(d) for d in tests])
            case {"or": [*tests]}:
                return OrTest([compile_test(d) for d in tests])
            case {"not": test}:
                return NotTest(compile_test(test))
            case _:
                pass
    elif isinstance(doc, str):
        return test_parser.parse(doc)

    raise ValueError(f"invalid test: {doc}")


def compile_action(doc: Any):
    if isinstance(doc, list):
        return ActionSequence([compile_action(d) for d in doc])
    elif isinstance(doc, str):
        return action_parser.parse(doc)

    raise ValueError(f"invalid action: {doc}")


def compile_rule(name: str, doc: Any):
    assert isinstance(doc, dict)
    return Rule(
        test=compile_test(doc["test"]),
        then=compile_action(doc["then"]),
    )
