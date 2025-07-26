import json
import tempfile
import pytest

from src.expectations.config.expectation import ExpectationSuiteConfig
from src.expectations.validators.column import ColumnNotNull
import sys
import src.expectations.validators.column as column_mod


def test_from_yaml(tmp_path):
    yaml_content = """
suite_name: s
engine: duck
table: t
expectations:
  - expectation_type: ColumnNotNull
    column: a
"""
    yml = tmp_path / "suite.yml"
    yml.write_text(yaml_content)

    cfg = ExpectationSuiteConfig.from_yaml(yml)
    assert isinstance(cfg, ExpectationSuiteConfig)


def test_from_json(tmp_path):
    data = {
        "suite_name": "s",
        "engine": "duck",
        "table": "t",
        "expectations": [
            {"expectation_type": "ColumnNotNull", "column": "a"}
        ],
    }
    path = tmp_path / "suite.json"
    path.write_text(json.dumps(data))
    cfg = ExpectationSuiteConfig.from_json(path)
    assert isinstance(cfg, ExpectationSuiteConfig)


def test_build_validators(tmp_path):
    yaml_content = """
suite_name: s
engine: duck
table: t
expectations:
  - expectation_type: ColumnNotNull
    column: a
"""
    path = tmp_path / "suite.yml"
    path.write_text(yaml_content)
    cfg = ExpectationSuiteConfig.from_yaml(path)
    sys.modules.setdefault("validator.validators.column", column_mod)
    validators = list(cfg.build_validators())
    assert len(validators) == 1
    _, _, v = validators[0]
    assert isinstance(v, ColumnNotNull)


def test_from_file_yaml_and_json(tmp_path):
    yaml_content = """
suite_name: s
engine: duck
table: t
expectations:
  - expectation_type: ColumnNotNull
    column: a
"""
    yml = tmp_path / "suite.yml"
    yml.write_text(yaml_content)
    cfg_yaml = ExpectationSuiteConfig.from_file(yml)
    assert isinstance(cfg_yaml, ExpectationSuiteConfig)

    data = {
        "suite_name": "s",
        "engine": "duck",
        "table": "t",
        "expectations": [{"expectation_type": "ColumnNotNull", "column": "a"}],
    }
    js = tmp_path / "suite.json"
    js.write_text(json.dumps(data))
    cfg_json = ExpectationSuiteConfig.from_file(js)
    assert isinstance(cfg_json, ExpectationSuiteConfig)


def test_from_file_unknown_extension(tmp_path):
    path = tmp_path / "suite.txt"
    path.write_text("invalid")
    with pytest.raises(ValueError):
        ExpectationSuiteConfig.from_file(path)


from src.expectations.config.expectation import _resolve_validator_class
from src.expectations.validators.table import RowCountValidator


def test_resolve_validator_class(monkeypatch):
    sys.modules.setdefault("validator.validators.column", column_mod)
    cls = _resolve_validator_class("ColumnNotNull")
    assert cls is ColumnNotNull
    cls2 = _resolve_validator_class(
        "src.expectations.validators.table.RowCountValidator"
    )
    assert cls2 is RowCountValidator
    with pytest.raises(AttributeError):
        _resolve_validator_class("Nope")
