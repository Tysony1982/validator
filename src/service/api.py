from __future__ import annotations

"""Minimal web service for managing validation suites and viewing runs."""

from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException

from src.expectations.config.expectation import ExpectationSuiteConfig
from src.expectations.runner import ValidationRunner
from src.expectations.store.base import BaseResultStore
from src.expectations.workflow import run_validations


class SuiteStore:
    """Simple file-based store for expectation suites."""

    def __init__(self, directory: str | Path):
        self.base_path = Path(directory)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def list_suites(self) -> List[str]:
        return [p.stem for p in self.base_path.glob("*.yml")]

    def load(self, name: str) -> ExpectationSuiteConfig:
        path = self.base_path / f"{name}.yml"
        if not path.exists():
            raise FileNotFoundError(name)
        return ExpectationSuiteConfig.from_yaml(path)

    def save(self, suite: ExpectationSuiteConfig) -> None:
        path = self.base_path / f"{suite.suite_name}.yml"
        path.write_text(suite.to_yaml())


class Service:
    """Wrap FastAPI app with runner and persistence."""

    def __init__(
        self,
        runner: ValidationRunner,
        result_store: BaseResultStore,
        suite_store: SuiteStore,
    ) -> None:
        self.runner = runner
        self.result_store = result_store
        self.suite_store = suite_store
        self.app = FastAPI(title="Validator Service")
        self._init_routes()

    # -------------------------------------------------------------- #
    # API definitions                                               #
    # -------------------------------------------------------------- #
    def _init_routes(self) -> None:
        app = self.app

        @app.get("/suites")
        def list_suites() -> List[str]:
            return self.suite_store.list_suites()

        @app.post("/suites")
        def create_suite(cfg: ExpectationSuiteConfig) -> None:
            self.suite_store.save(cfg)

        @app.post("/runs/{suite_name}")
        def run_suite(suite_name: str):
            suite = self.suite_store.load(suite_name)
            run, results = run_validations(
                suite_name=suite.suite_name,
                bindings=suite.build_validators(),
                runner=self.runner,
                store=self.result_store,
            )
            return {"run": run, "results": results}

        @app.get("/runs")
        def list_runs():
            try:
                conn = self.result_store.connection
            except AttributeError:
                raise HTTPException(status_code=400, detail="Store does not expose connection")
            df = conn.execute("SELECT * FROM runs ORDER BY started_at DESC").fetchdf()
            return df.to_dict(orient="records")

        @app.get("/runs/{run_id}")
        def get_results(run_id: str):
            try:
                conn = self.result_store.connection
            except AttributeError:
                raise HTTPException(status_code=400, detail="Store does not expose connection")
            df = conn.execute(
                "SELECT * FROM results WHERE run_id = ?", [run_id]
            ).fetchdf()
            return df.to_dict(orient="records")


__all__ = ["Service", "SuiteStore"]
