from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

from src.expectations.config.expectation import SLAConfig
from src.expectations.result_model import RunMetadata, ValidationResult
from src.expectations.stats import MetricStat
from .base import BaseResultStore


class FileResultStore(BaseResultStore):
    """Persist validation artefacts to a directory as JSON files."""

    def __init__(self, directory: str | Path):
        self.base_path = Path(directory)
        self.base_path.mkdir(parents=True, exist_ok=True)
        for sub in ("runs", "results", "slas", "statistics"):
            (self.base_path / sub).mkdir(exist_ok=True)

    # ------------------------------------------------------------------ #
    # BaseResultStore interface
    # ------------------------------------------------------------------ #
    def persist_run(
        self,
        run: RunMetadata,
        results: Sequence[ValidationResult],
        sla_config: SLAConfig | None = None,
    ) -> None:
        run_path = self.base_path / "runs" / f"{run.run_id}.json"
        run_path.write_text(run.model_dump_json())

        res_path = self.base_path / "results" / f"{run.run_id}.jsonl"
        with res_path.open("w") as fh:
            for r in results:
                fh.write(r.model_dump_json())
                fh.write("\n")

        if run.sla_name and sla_config is not None:
            sla_path = self.base_path / "slas" / f"{run.sla_name}.json"
            sla_path.write_text(sla_config.model_dump_json())

    def persist_stats(self, run: RunMetadata, stats: Sequence[MetricStat]) -> None:
        stats_path = self.base_path / "statistics" / f"{run.run_id}.jsonl"
        with stats_path.open("w") as fh:
            for s in stats:
                fh.write(s.model_dump_json())
                fh.write("\n")

    def close(self) -> None:  # pragma: no cover - nothing to close
        pass
