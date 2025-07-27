"""High level helpers for executing and persisting validations."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence, Tuple

from src.expectations.runner import ValidationRunner, ValidatorBinding
from src.expectations.store.base import BaseResultStore
from src.expectations.result_model import RunMetadata, ValidationResult
from src.expectations.config.expectation import SLAConfig


def run_validations(
    *,
    suite_name: str,
    bindings: Sequence[ValidatorBinding],
    runner: ValidationRunner,
    store: BaseResultStore,
    sla_config: SLAConfig | None = None,
) -> Tuple[RunMetadata, Sequence[ValidationResult]]:
    """Execute *bindings* using *runner* and persist results via *store*.

    A :class:`RunMetadata` object is created automatically and its
    ``finished_at`` timestamp is set right before persisting.
    ``sla_config`` can be provided when the run is associated with an SLA.
    """

    engine_name = bindings[0][0] if bindings else None
    schema = None
    if bindings and "." in bindings[0][1]:
        schema = bindings[0][1].rsplit(".", 1)[0]

    run = RunMetadata(
        suite_name=suite_name,
        sla_name=sla_config.sla_name if sla_config else None,
        engine_name=engine_name,
        db_schema=schema,
    )
    results = runner.run(bindings, run_id=run.run_id)
    for r in results:
        r.engine_name = run.engine_name
        r.db_schema = run.db_schema
    run.finished_at = datetime.utcnow()
    store.persist_run(run, results, sla_config)
    return run, results

