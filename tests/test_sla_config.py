import sys
import src.expectations.validators.column as column_mod

from src.expectations.config.expectation import SLAConfig
from src.expectations.validators.column import ColumnNotNull


def _sample_yaml():
    return """
sla_name: sla
suites:
  - suite_name: s1
    engine: duck
    table: t1
    expectations:
      - expectation_type: ColumnNotNull
        column: a
  - suite_name: s2
    engine: duck
    table: t2
    expectations:
      - expectation_type: ColumnNotNull
        column: b
"""


def test_sla_from_yaml(tmp_path):
    path = tmp_path / "sla.yml"
    path.write_text(_sample_yaml())
    cfg = SLAConfig.from_yaml(path)
    assert cfg.sla_name == "sla"
    assert len(cfg.suites) == 2


def test_sla_build_validators(tmp_path):
    path = tmp_path / "sla.yml"
    path.write_text(_sample_yaml())
    cfg = SLAConfig.from_yaml(path)
    validators = list(cfg.build_validators())
    assert len(validators) == 2
    assert isinstance(validators[0][2], ColumnNotNull)
