"""Shared config/schema symbols for source DB generation internals."""

from .config import CSDGParameters, DEFAULT_CSDG_PARAMS
from .conversions import apply_lead_conversions
from .orders import build_order_payload, persist_orders_and_items
from .schema import CORE_SCHEMA_SQL, LEAD_CONVERSIONS_SCHEMA_SQL

__all__ = [
    "CSDGParameters",
    "DEFAULT_CSDG_PARAMS",
    "apply_lead_conversions",
    "build_order_payload",
    "CORE_SCHEMA_SQL",
    "LEAD_CONVERSIONS_SCHEMA_SQL",
    "persist_orders_and_items",
]
