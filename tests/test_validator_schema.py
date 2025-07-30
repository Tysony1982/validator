import importlib
import json
import sys
from pathlib import Path


def test_validator_schema_contains_known_validator():
    sys.modules.pop('src.expectations.validators.schema', None)
    sys.modules.pop('src.expectations.validators', None)
    pkg = importlib.import_module('src.expectations.validators')

    out_file = Path(pkg.__file__).with_name('validators.json')
    assert out_file.exists(), 'validators.json was not created'

    data = json.loads(out_file.read_text())
    assert 'ColumnNullPct' in data
