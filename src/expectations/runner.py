"""
validator.runner
~~~~~~~~~~~~~~~~

Single entry-point that executes both *metric* and *custom* validators.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from sqlglot import exp

from src.expectations.engines.base import BaseEngine
from src.expectations.metrics.batch_builder import MetricBatchBuilder
from src.expectations.result_model import ValidationResult
from src.expectations.validators.base import ValidatorBase

# --------------------------------------------------------------------------- #
# Helper dataclass                                                            #
# --------------------------------------------------------------------------- #
ValidatorBinding = Tuple[str, str, ValidatorBase]  # (engine_key, table, validator)


# --------------------------------------------------------------------------- #
# Runner                                                                      #
# --------------------------------------------------------------------------- #
class ValidationRunner:
    def __init__(self, engine_map: Dict[str, BaseEngine]):
        self.engine_map = engine_map

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def run(self, bindings: Sequence[ValidatorBinding]) -> List[ValidationResult]:
        metric_groups: Dict[Tuple[str, str], List[ValidatorBase]] = defaultdict(list)
        custom_bindings: List[ValidatorBinding] = []

        # Split upfront
        for eng_key, table, v in bindings:
            if v.kind() == "metric":
                metric_groups[(eng_key, table)].append(v)
            else:
                custom_bindings.append((eng_key, table, v))

        results: List[ValidationResult] = []

        # -- metric validators (batched) --------------------------------
        for (eng_key, table), validators in metric_groups.items():
            engine = self.engine_map[eng_key]
            requests = [v.metric_request() for v in validators]
            sql = MetricBatchBuilder(
                table=table,
                requests=requests,
                dialect=engine.get_dialect(),
            ).sql()
            try:
                df: pd.DataFrame = engine.run_sql(sql)
                row = df.iloc[0]
                for v in validators:
                    val = row[v.runtime_id]
                    ok = v.interpret(val)
                    results.append(
                        ValidationResult(
                            run_id="",  # set by caller
                            validator=v.__class__.__name__,
                            table=table,
                            column=getattr(v, "column", None),
                            metric=v.metric_request().metric,
                            success=ok,
                            value=val,
                            filter_sql=v.where_condition,
                        )
                    )
            except Exception as exc:  # pylint: disable=broad-except
                for v in validators:
                    results.append(
                        ValidationResult(
                            run_id="",
                            validator=v.__class__.__name__,
                            table=table,
                            column=getattr(v, "column", None),
                            metric=v.metric_request().metric,
                            success=False,
                            value=None,
                            filter_sql=v.where_condition,
                            details={"error": str(exc)},
                        )
                    )

        # -- custom validators ------------------------------------------
        for eng_key, table, v in custom_bindings:
            engine = self.engine_map[eng_key]
            sql_or_ast = v.custom_sql(table)
            sql = sql_or_ast.sql() if isinstance(sql_or_ast, exp.Expression) else str(sql_or_ast)
            try:
                df = engine.run_sql(sql)
                # convention: custom validators look at *first scalar* of first row
                raw_val = df.iloc[0, 0] if not df.empty else None
                ok = v.interpret(raw_val)
            except Exception as exc:  # pylint: disable=broad-except
                ok = False
                raw_val = None
                err = str(exc)

            results.append(
                ValidationResult(
                    run_id="",
                    validator=v.__class__.__name__,
                    table=table,
                    column=getattr(v, "column", None),
                    success=ok,
                    value=raw_val,
                    details={} if ok else {"error": err},
                )
            )

        return results


# --------------------------------------------------------------------------- #
# Smoke test (pytest will pick up)                                            #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import duckdb

    # 1) quick DuckDB engine impl -------------------------------------
    class DuckEngine(BaseEngine):
        def __init__(self):
            self._conn = duckdb.connect(":memory:")
            self._dialect = "duckdb"

        def run_sql(self, sql):
            return self._conn.execute(str(sql)).fetchdf()

        def list_columns(self, table):
            return [row[0] for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()]

        def get_dialect(self):
            return self._dialect

        def close(self):
            self._conn.close()

    eng = DuckEngine()
    eng.run_sql("CREATE TABLE t(a INT, b INT); INSERT INTO t VALUES (1,1),(NULL,2),(NULL,3);")

    # 2) tiny validator ------------------------------------------------
    from validators.base import ValidatorBase
    from metrics.batch_builder import MetricRequest

    class ColumnNullPct(ValidatorBase):
        def __init__(self, column):
            super().__init__()
            self.column = column

        @classmethod
        def kind(cls):
            return "metric"

        def metric_request(self):
            return MetricRequest(column=self.column, metric="null_pct", alias=self.runtime_id)

        def interpret(self, value):
            return float(value) == 0.0

    bindings = [("duck", "t", ColumnNullPct("a"))]
    vr = ValidationRunner({"duck": eng}).run(bindings)
    print(vr)
