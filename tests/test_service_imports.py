import importlib


def test_service_module_imports():
    """Modules should be importable without path hacks."""
    importlib.import_module("src.service.api")
    importlib.import_module("src.service.streamlit_app")
