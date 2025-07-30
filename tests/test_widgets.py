import streamlit as st
import pytest

from src.service.widgets import widget_for


@pytest.mark.parametrize(
    "meta, expected",
    [
        ({"type": "choice", "options": [1]}, "selectbox"),
        ({"type": "number"}, "number_input"),
        ({"type": "bool"}, "checkbox"),
        ({}, "text_input"),
    ],
)
def test_widget_for(monkeypatch, meta, expected):
    calls = []

    monkeypatch.setattr(st, "selectbox", lambda *a, **kw: calls.append("selectbox"))
    monkeypatch.setattr(st, "number_input", lambda *a, **kw: calls.append("number_input"))
    monkeypatch.setattr(st, "checkbox", lambda *a, **kw: calls.append("checkbox"))
    monkeypatch.setattr(st, "text_input", lambda *a, **kw: calls.append("text_input"))

    widget_for("field", meta, "key")
    assert calls == [expected]

