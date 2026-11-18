"""Orchestrate package. Exposes `defs` so `dagster dev -m orchestrate` finds it."""

from orchestrate.definitions import defs

__all__ = ["defs"]
