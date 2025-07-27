import importlib
import inspect
import pkgutil
from pathlib import Path

from src.expectations.validators.base import ValidatorBase


def generate_validator_docs(app):
    project_root = Path(app.srcdir).resolve().parent
    validators_pkg = project_root / 'src' / 'expectations' / 'validators'
    classes = []

    for info in pkgutil.iter_modules([str(validators_pkg)]):
        if info.name.startswith('_'):
            continue
        module = importlib.import_module(f'src.expectations.validators.{info.name}')
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue
            if not issubclass(obj, ValidatorBase) or obj in {ValidatorBase}:
                continue
            if name == 'ColumnMetricValidator':
                continue
            sig = inspect.signature(obj.__init__)
            params = list(sig.parameters.values())[1:]  # drop self
            sig_str = '(' + ', '.join(str(p) for p in params) + ')'
            doc = inspect.getdoc(obj) or ''
            classes.append((name, sig_str, doc))

    classes.sort(key=lambda t: t[0])
    lines = ['Validator Reference', '===================', '']
    for name, sig, doc in classes:
        lines.append(name)
        lines.append('-' * len(name))
        lines.append('')
        lines.append(f'**Signature**::')
        lines.append('')
        lines.append(f'    {name}{sig}')
        lines.append('')
        if doc:
            lines.append(doc)
            lines.append('')
    out_file = Path(app.srcdir) / 'validators.rst'
    out_file.write_text('\n'.join(lines))


def setup(app):
    app.connect('builder-inited', generate_validator_docs)
    return {'version': '0.1'}
