import pandas as pd
from src.expectations.engines.duckdb import DuckDBEngine
from src.expectations.validators.column import ColumnNotNull
from src.expectations.runner import ValidationRunner
from src.expectations.result_model import RunMetadata

# 1) set-up a tiny table
eng = DuckDBEngine()
eng.register_dataframe("t", pd.DataFrame({"a": [1, 2, None], "b": [1, 2, 3]}))

# 2) run a ColumnNotNull check on column 'b'
bindings = [("duck", "t", ColumnNotNull(column="b"))]
runner   = ValidationRunner({"duck": eng})
run = RunMetadata(suite_name="demo")
print(runner.run(bindings, run_id=run.run_id))
