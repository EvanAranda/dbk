from dbk.core import rules


def test_resolve_local_references():
    yaml = """
    test:
        tests:
            foo: desc contains foo
        actions:
            foo: set desc to foo
        rules:
            foo:
                test: ::foo
                then: ::foo
    """
    scope = rules.compile_rules(yaml)

    ruleset = scope.rulesets["test"]

    foo_test_ref = ruleset.rules["foo"].test
    assert isinstance(foo_test_ref, rules.ReferencedTest)
    assert foo_test_ref.bound is None

    foo_action_ref = ruleset.rules["foo"].then
    assert isinstance(foo_action_ref, rules.ReferencedAction)
    assert foo_action_ref.bound is None

    rules.resolve_references(scope)
    assert foo_test_ref.bound is ruleset.tests["foo"]
    assert foo_action_ref.bound is ruleset.actions["foo"]


def test_resolve_external_references():
    yaml = """
    external:
        tests:
            bar: amount is 0
    test:
        tests:
            foo: 
                - external::bar
                - desc contains foo
    """

    scope = rules.compile_rules(yaml)
    rules.resolve_references(scope)

    bar_test = scope.rulesets["external"].tests["bar"]
    foo_test = scope.rulesets["test"].tests["foo"]

    assert isinstance(foo_test, rules.AndTest)
    assert isinstance(foo_test.tests[0], rules.ReferencedTest)
    assert foo_test.tests[0].bound is bar_test
