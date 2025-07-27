import os
from pathlib import Path

import pandas as pd

from src.expectations.engines.file import FileEngine


def test_file_engine_csv(tmp_path):
    path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(path, index=False)

    eng = FileEngine(path, table="t")
    cols = set(eng.list_columns("t"))
    assert cols == {"a", "b"}
    df = eng.run_sql("SELECT COUNT(*) AS c FROM t")
    assert df.iloc[0]["c"] == 2
    eng.close()


def test_file_engine_glob(tmp_path):
    for i in range(2):
        pd.DataFrame({"a": [i]}).to_csv(tmp_path / f"f{i}.csv", index=False)

    eng = FileEngine(str(tmp_path / "f*.csv"), table="t")
    df = eng.run_sql("SELECT COUNT(*) AS c FROM t")
    assert df.iloc[0]["c"] == 2
    eng.close()


def test_file_engine_cleanup(tmp_path):
    path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1]}).to_csv(path, index=False)

    eng = FileEngine(path, table="t")
    eng.close()

    eng2 = FileEngine(path, table="t")
    df = eng2.run_sql("SELECT COUNT(*) AS c FROM t")
    assert df.iloc[0]["c"] == 1
    eng2.close()


def test_file_engine_file_metadata(tmp_path):
    path1 = tmp_path / "one.csv"
    path2 = tmp_path / "two.csv"

    pd.DataFrame({"a": [1]}).to_csv(path1, index=False)
    pd.DataFrame({"a": [2, 3]}).to_csv(path2, index=False)

    eng = FileEngine(str(tmp_path / "*.csv"), table="t")
    meta = eng.file_metadata

    expected_paths = {path1.resolve(), path2.resolve()}
    returned_paths = {Path(m["path"]) for m in meta}
    assert returned_paths == expected_paths

    for m in meta:
        p = Path(m["path"])
        assert m["size"] == os.stat(p).st_size

    eng.close()

    path1.unlink()
    path2.unlink()
