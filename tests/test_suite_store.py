import pytest
from src.service.api import SuiteStore
from src.expectations.config.expectation import ExpectationSuiteConfig
from pathlib import Path


@pytest.mark.parametrize("invalid_name", ["../evil", "foo/bar", "foo\\bar"])
def test_save_rejects_invalid_names(tmp_path, invalid_name):
    store = SuiteStore(tmp_path / "suites")
    cfg = ExpectationSuiteConfig(suite_name=invalid_name, engine="duck", table="t", expectations=[])
    with pytest.raises(ValueError):
        store.save(cfg)
    assert not any((tmp_path / "suites").glob("*") )
    assert not list(tmp_path.glob("*.yml"))


@pytest.mark.parametrize("invalid_name", ["../evil", "foo/bar", "foo\\bar"])
def test_load_rejects_invalid_names(tmp_path, invalid_name):
    store = SuiteStore(tmp_path / "suites")
    outside = tmp_path / f"{Path(invalid_name).name}.yml"
    outside_cfg = ExpectationSuiteConfig(suite_name="outside", engine="duck", table="t", expectations=[])
    outside.write_text(outside_cfg.to_yaml())

    with pytest.raises(ValueError):
        store.load(invalid_name)
    assert outside.exists()  # ensure file wasn't altered
    assert not any((tmp_path / "suites").glob("*") )
