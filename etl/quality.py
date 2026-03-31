"""
Data Quality checks — applied at the staging layer.
Each check returns (passed_df, failed_df) for full traceability.
"""

import logging
from dataclasses import dataclass, field
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class DQReport:
    total: int = 0
    passed: int = 0
    failed: int = 0
    checks: list = field(default_factory=list)

    def log(self):
        logger.info(f"[DQ] Total={self.total} | Passed={self.passed} | Failed={self.failed}")
        for c in self.checks:
            logger.info(f"  └─ {c}")

    @property
    def pass_rate(self) -> float:
        return round(self.passed / self.total * 100, 2) if self.total else 0


def check_sales(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, DQReport]:
    report = DQReport(total=len(df))
    df = df.copy()
    df["_dq_passed"] = True
    df["_dq_notes"]  = ""

    # ── Rule 1: order_date must be a valid date ──────────────────────────────
    invalid_date_mask = pd.to_datetime(df["order_date"], errors="coerce").isna()
    df.loc[invalid_date_mask, "_dq_passed"] = False
    df.loc[invalid_date_mask, "_dq_notes"] += "[ERR] invalid order_date; "
    report.checks.append(f"invalid_date: {invalid_date_mask.sum()} rows")

    # ── Rule 2: quantity must be > 0 ─────────────────────────────────────────
    qty = pd.to_numeric(df["quantity"], errors="coerce")
    invalid_qty_mask = qty.isna() | (qty <= 0)
    df.loc[invalid_qty_mask, "_dq_passed"] = False
    df.loc[invalid_qty_mask, "_dq_notes"] += "[ERR] quantity <= 0 or null; "
    report.checks.append(f"invalid_quantity: {invalid_qty_mask.sum()} rows")

    # ── Rule 3: unit_price must be >= 0 ──────────────────────────────────────
    price = pd.to_numeric(df["unit_price"], errors="coerce")
    invalid_price_mask = price.isna() | (price < 0)
    df.loc[invalid_price_mask, "_dq_passed"] = False
    df.loc[invalid_price_mask, "_dq_notes"] += "[ERR] unit_price < 0 or null; "
    report.checks.append(f"invalid_price: {invalid_price_mask.sum()} rows")

    # ── Rule 4: FK references not null ───────────────────────────────────────
    null_fk_mask = df["customer_id"].isna() | df["product_id"].isna()
    df.loc[null_fk_mask, "_dq_passed"] = False
    df.loc[null_fk_mask, "_dq_notes"] += "[ERR] null FK (customer or product); "
    report.checks.append(f"null_foreign_keys: {null_fk_mask.sum()} rows")

    passed_df = df[df["_dq_passed"]].copy()
    failed_df = df[~df["_dq_passed"]].copy()

    report.passed = len(passed_df)
    report.failed = len(failed_df)
    report.log()
    return passed_df, failed_df, report
