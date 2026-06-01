import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data"
BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver"

# SELIC daily rate boundaries: historical range is roughly 0.008% – 0.065% per day
_VALOR_MIN = 0.0
_VALOR_MAX = 0.2

# Max legitimate calendar gap: Christmas + New Year holiday block in Brazil (~10 days)
_MAX_DATE_GAP_DAYS = 12


def _validate_bronze(df: pd.DataFrame) -> None:
    """Quality gate: assert Bronze schema and completeness before transforming."""
    required = {"data", "valor"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Bronze is missing required columns: {missing}")

    null_counts = df[list(required)].isnull().sum()
    if null_counts.any():
        logger.warning("Nulls detected in Bronze:\n%s", null_counts[null_counts > 0])


def transform_selic() -> int:
    """Clean and standardize Bronze → Silver.

    Transformations applied:
    - 'data': string dd/MM/yyyy  →  datetime64
    - 'valor': string            →  float64 (coerce unparseable → NaN, then drop)
    - Derived columns: ano, mes, ano_mes, dia_semana
    - Sort ascending by date

    Quality gate: validates value range and year coverage before saving.

    Returns:
        Number of rows saved to Silver.
    """
    bronze_path = BRONZE_PATH / "selic_raw.parquet"
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

    df = pd.read_parquet(bronze_path)
    logger.info("Loaded Bronze: %d rows", len(df))

    _validate_bronze(df)

    # --- Type conversions ---
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    # --- Null treatment ---
    null_count = df[["data", "valor"]].isnull().sum().sum()
    if null_count > 0:
        logger.warning("Dropping %d rows with unparseable values", null_count)
        df = df.dropna(subset=["data", "valor"])

    # --- Derived columns ---
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    df["ano_mes"] = df["data"].dt.to_period("M").astype(str)
    df["dia_semana"] = df["data"].dt.day_name()

    df = df.sort_values("data").reset_index(drop=True)

    # --- Quality gate ---
    out_of_range = ~df["valor"].between(_VALOR_MIN, _VALOR_MAX)
    if out_of_range.any():
        raise ValueError(
            f"Silver quality gate failed: {out_of_range.sum()} rows with 'valor' "
            f"outside [{_VALOR_MIN}, {_VALOR_MAX}]"
        )

    unexpected_years = set(df["ano"].unique()) - set(range(2020, 2025))
    if unexpected_years:
        raise ValueError(f"Silver quality gate failed: unexpected years {unexpected_years}")

    duplicates = df["data"].duplicated()
    if duplicates.any():
        raise ValueError(
            f"Silver quality gate failed: {duplicates.sum()} duplicate dates detected"
        )

    max_gap_val = df["data"].diff().dt.days.max()
    if pd.notna(max_gap_val) and int(max_gap_val) > _MAX_DATE_GAP_DAYS:
        raise ValueError(
            f"Silver quality gate failed: gap of {int(max_gap_val)} days detected in date series "
            f"(max allowed: {_MAX_DATE_GAP_DAYS})"
        )

    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    output_path = SILVER_PATH / "selic_trusted.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")

    logger.info("Silver saved → %s (%d rows)", output_path, len(df))
    return len(df)
