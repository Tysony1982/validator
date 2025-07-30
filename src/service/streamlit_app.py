"""Simple Streamlit UI on top of the :mod:`validator.service` API."""

from __future__ import annotations

import os
from typing import List

import pandas as pd
import requests
import streamlit as st

try:  # optional dependency used for nicer YAML editing
    from streamlit_ace import st_ace  # type: ignore
except Exception:  # pragma: no cover - dev dependency
    st_ace = None  # type: ignore

import duckdb

from src.expectations.config.expectation import ExpectationSuiteConfig


SERVICE_URL = os.getenv("SERVICE_URL", "http://localhost:8000")
"""Base URL of the running :class:`validator.service.Service`."""

DUCKDB_PATH = os.getenv("RESULT_DB")
"""Optional path to a DuckDB database for read-only history lookup."""


@st.cache_data(show_spinner=False)
def _load_runs() -> pd.DataFrame:
    """Fetch validation runs either via REST or directly from DuckDB."""

    if DUCKDB_PATH:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        df = conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC"
        ).fetchdf()
        conn.close()
        return df

    try:
        resp = requests.get(f"{SERVICE_URL}/runs")
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:  # pragma: no cover - network failure
        st.error(str(exc))
        return pd.DataFrame()

    return pd.DataFrame(resp.json())


@st.cache_data(show_spinner=False)
def _load_results(run_id: str) -> pd.DataFrame:
    """Fetch validation results for *run_id*."""

    if DUCKDB_PATH:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        df = conn.execute(
            "SELECT * FROM results WHERE run_id = ?", [run_id]
        ).fetchdf()
        conn.close()
        return df

    try:
        resp = requests.get(f"{SERVICE_URL}/runs/{run_id}")
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:  # pragma: no cover - network failure
        st.error(str(exc))
        return pd.DataFrame()

    return pd.DataFrame(resp.json())


def _list_suites() -> List[str]:
    resp = requests.get(f"{SERVICE_URL}/suites")
    resp.raise_for_status()
    return list(resp.json())


def _save_suite(text: str) -> None:
    cfg = ExpectationSuiteConfig.model_validate_yaml(text)
    resp = requests.post(f"{SERVICE_URL}/suites", json=cfg.model_dump())
    resp.raise_for_status()


def _trigger_run(suite_name: str) -> None:
    resp = requests.post(f"{SERVICE_URL}/runs/{suite_name}")
    resp.raise_for_status()


def page_runs() -> None:
    st.header("Validation Runs")
    # NOTE: ``st.autorefresh`` was renamed to ``st_autorefresh`` in newer
    # Streamlit versions. Importing the function directly keeps compatibility
    # with older releases that may not expose it as a method on ``st``.
    try:
        from streamlit import st_autorefresh  # type: ignore
    except Exception:  # pragma: no cover - missing in very old versions
        st_autorefresh = None  # type: ignore

    if st_autorefresh:
        st_autorefresh(interval=30_000)

    df = _load_runs()
    if df.empty:
        st.info("No runs found")
        return

    suite = st.selectbox(
        "Suite", ["All"] + sorted(df["suite_name"].unique().tolist())
    )
    if suite != "All":
        df = df[df["suite_name"] == suite]

    st.dataframe(df, use_container_width=True)

    run_id = st.selectbox("Show results for run", ["None"] + df["run_id"].tolist())
    if run_id and run_id != "None":
        res = _load_results(run_id)
        if res.empty:
            st.info("No results found")
        else:
            st.dataframe(res, use_container_width=True)


def page_editor() -> None:
    st.header("Suite Builder")
    suites = _list_suites()
    choice = st.selectbox("Existing suites", ["New suite"] + suites)
    text = ""
    if choice != "New suite":
        resp = requests.get(f"{SERVICE_URL}/suites/{choice}")
        if resp.status_code == 200:
            text = resp.text

    if st_ace:
        content = st_ace(value=text, language="yaml", theme="monokai")
    else:
        content = st.text_area("Suite YAML", value=text, height=300)

    if st.button("Save suite"):
        try:
            _save_suite(content)
        except Exception as exc:  # pragma: no cover - user feedback
            st.error(str(exc))
        else:
            st.success("Saved")

    if choice != "New suite" and st.button("Run now"):
        try:
            _trigger_run(choice)
        except Exception as exc:  # pragma: no cover
            st.error(str(exc))
        else:
            st.success("Run triggered")


def main() -> None:  # pragma: no cover - interactive
    st.title("Validator UI")
    page = st.sidebar.selectbox("Page", ["Runs", "Suite Builder"])
    if page == "Runs":
        page_runs()
    else:
        page_editor()


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()

