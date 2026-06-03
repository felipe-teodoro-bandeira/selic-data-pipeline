import logging
import os
from pathlib import Path
from typing import TypedDict

import pandas as pd

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data"
SILVER_PATH = DATA_ROOT / "silver"
GOLD_PATH = DATA_ROOT / "gold"


class GoldSummary(TypedDict):
    monthly_rows: int
    annual_rows: int


def _compound_rate(series: pd.Series) -> float:
    """Compute compound accumulated rate from daily percentual rates.

    Formula: (∏(1 + r_i / 100) − 1) × 100

    This is the correct financial calculation for SELIC: each daily rate
    compounds onto the previous, matching how BCB publishes the effective
    period rate.
    """
    return ((1 + series / 100).prod() - 1) * 100


def aggregate_selic() -> GoldSummary:
    """Compute monthly and annual SELIC metrics from Silver → Gold.

    Monthly output (selic_mensal.parquet):
        ano_mes, media_diaria_mensal, taxa_acumulada_mensal,
        variacao_mensal_pp, dias_uteis, data_inicio, data_fim

    Annual output (selic_anual.parquet):
        ano, media_diaria_anual, taxa_acumulada_anual, dias_uteis

    Returns:
        Dict with row counts for each output file.
    """
    silver_path = SILVER_PATH / "selic_trusted.parquet"
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver file not found: {silver_path}")

    df = pd.read_parquet(silver_path)
    logger.info("Loaded Silver: %d rows", len(df))

    # --- Monthly aggregation ---
    monthly = (
        df.groupby("ano_mes", sort=True)
        .agg(
            media_diaria_mensal=("valor", "mean"),
            taxa_acumulada_mensal=("valor", _compound_rate),
            dias_uteis=("valor", "count"),
            data_inicio=("data", "min"),
            data_fim=("data", "max"),
        )
        .reset_index()
    )
    # Variation in percentage points vs previous month's compound rate
    monthly["variacao_mensal_pp"] = monthly["taxa_acumulada_mensal"].diff()

    # Explicit boolean flag: True only for the first period (no previous month to diff against).
    # Prevents silent NaN propagation in BI tools that filter out NULL rows.
    monthly["is_base_month"] = monthly["variacao_mensal_pp"].isna()

    # Sort by actual date, not lexicographic string — guards against future format changes
    monthly = monthly.sort_values("data_inicio").reset_index(drop=True)

    # --- Annual aggregation ---
    annual = (
        df.groupby("ano", sort=True)
        .agg(
            media_diaria_anual=("valor", "mean"),
            taxa_acumulada_anual=("valor", _compound_rate),
            dias_uteis=("valor", "count"),
        )
        .reset_index()
    )

    # --- Quality gates ---
    min_days = monthly["dias_uteis"].min()
    if min_days < 10:
        raise ValueError(
            f"Gold quality gate failed: month with only {min_days} business days — "
            "possible data gap in Silver"
        )

    invalid_annual = ~annual["taxa_acumulada_anual"].between(0, 25)
    if invalid_annual.any():
        bad = annual.loc[invalid_annual, ["ano", "taxa_acumulada_anual"]]
        raise ValueError(f"Gold quality gate failed: annual rates out of range:\n{bad}")

    # --- Persist ---
    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    monthly_path = GOLD_PATH / "selic_mensal.parquet"
    annual_path = GOLD_PATH / "selic_anual.parquet"

    monthly.to_parquet(monthly_path, index=False, engine="pyarrow")
    annual.to_parquet(annual_path, index=False, engine="pyarrow")

    logger.info("Gold monthly → %s (%d rows)", monthly_path, len(monthly))
    logger.info("Gold annual  → %s (%d rows)", annual_path, len(annual))

    return GoldSummary(monthly_rows=len(monthly), annual_rows=len(annual))
