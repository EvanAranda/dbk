import pytest

from dbk.core import rules


@pytest.fixture
def new_rule_set():
    return rules.RuleSet("test")


class TestCompileTests:
    def test_compile_field_test(self):
        t = rules.compile_test("desc contains foo")
        assert isinstance(t, rules.FieldTest)

    def test_compile_list_of_tests(self):
        t = rules.compile_test(["desc contains foo", "amount is 10"])
        assert isinstance(t, rules.AndTest)

    def test_compile_or_dict(self):
        doc = {
            "or": [
                "desc contains foo",
                "amount is 10",
            ]
        }
        t = rules.compile_test(doc)
        assert isinstance(t, rules.OrTest)

    def test_compile_reference(self):
        t = rules.compile_test("::foo")
        assert isinstance(t, rules.ReferencedTest)
        assert t.ident == rules.Identifier(["foo"])

    def test_compile_scoped_reference(self):
        t = rules.compile_test("foo::bar")
        assert isinstance(t, rules.ReferencedTest)
        assert t.ident == rules.Identifier(["foo", "bar"])


class TestCompileActions:
    def test_compile_set_field(self):
        a = rules.compile_action("set desc to foo")
        assert isinstance(a, rules.SetField)
        assert isinstance(a.value, rules.Literal)

    def test_use_ruleset(self):
        a = rules.compile_action("use foo_ruleset")
        assert isinstance(a, rules.UseRuleSet)
        assert isinstance(a.ruleset, rules.ReferencedRuleSet)
        assert a.ruleset.ident == rules.Identifier(["foo_ruleset"])
        assert a.ruleset.bound is None


class TestCompileRules:
    def test_compile(self):
        doc = {
            "test": "desc contains foo",
            "then": "set desc to foo",
        }
        r = rules.compile_rule("foo", doc)
        assert isinstance(r, rules.Rule)


class TestCompileYaml:
    def test_compile(self):
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
        scope = rules.Scope(rules.compile_rules(yaml))
        assert len(scope.rulesets) == 1

        ruleset = scope.rulesets["test"]
        assert ruleset.name == "test"
        assert "foo" in ruleset.tests
        assert "foo" in ruleset.actions
        assert "foo" in ruleset.rules
