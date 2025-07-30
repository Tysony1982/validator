import importlib
import inspect
import json
import pkgutil
from pathlib import Path
from typing import get_type_hints

from .base import ValidatorBase


def _discover_validators():
    pkg_path = Path(__file__).resolve().parent
    validators = {}

    for info in pkgutil.iter_modules([str(pkg_path)]):
        if info.name.startswith("_") or info.name == "schema":
            continue
        module = importlib.import_module(f"{__package__}.{info.name}")
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue
            if not issubclass(obj, ValidatorBase) or obj in {ValidatorBase}:
                continue
            if name == "ColumnMetricValidator":
                continue
            doc = inspect.getdoc(obj) or ""
            sig = inspect.signature(obj.__init__)
            hints = get_type_hints(obj.__init__)
            params = []
            for p in list(sig.parameters.values())[1:]:
                meta = {"name": p.name}
                if p.name in hints:
                    meta["type"] = str(hints[p.name])
                if p.default is not inspect.Parameter.empty:
                    meta["default"] = p.default
                params.append(meta)
            validators[name] = {"doc": doc, "params": params}
    return validators


def _write_schema(data):
    pkg_path = Path(__file__).resolve().parent
    out_file = pkg_path / "validators.json"
    if out_file.exists():
        return
    out_file.write_text(json.dumps(data, indent=2, default=str))


schema = _discover_validators()
_write_schema(schema)

