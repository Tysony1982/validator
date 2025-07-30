from __future__ import annotations

"""Reusable Streamlit widgets."""

from typing import Any, Dict

import streamlit as st


def widget_for(name: str, meta: Dict[str, Any], key: str):
    """Return a Streamlit input widget for *name* based on *meta*."""

    field_type = meta.get("type")
    label = meta.get("label", name)
    if field_type == "choice":
        options = meta.get("options", [])
        return st.selectbox(label, options, key=key)
    if field_type == "number":
        return st.number_input(label, key=key)
    if field_type == "bool":
        return st.checkbox(label, key=key)
    return st.text_input(label, key=key)
