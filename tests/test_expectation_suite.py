import json
import tempfile

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
