import pandas as pd
import pytest

from gold.aggregate import _compound_rate, aggregate_selic


def _write_silver(tmp_path, df: pd.DataFrame):
    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(silver_dir / "selic_trusted.parquet", index=False)
    return silver_dir


def _make_silver_df(months: int = 2, days_per_month: int = 15, valor: float = 0.0267) -> pd.DataFrame:
    """Two months of business-day data with uniform daily rate."""
    starts = ["2020-01-02", "2020-02-03"][:months]
    dates = []
    for start in starts:
        dates.extend(pd.date_range(start, periods=days_per_month, freq="B").tolist())
    df = pd.DataFrame({"data": dates, "valor": [valor] * len(dates)})
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    df["ano_mes"] = df["data"].dt.to_period("M").astype(str)
    df["dia_semana"] = df["data"].dt.day_name()
    return df


class TestCompoundRate:
    def test_single_rate(self):
        result = _compound_rate(pd.Series([1.0]))
        assert abs(result - 1.0) < 1e-9

    def test_two_equal_rates(self):
        expected = ((1.01 * 1.01) - 1) * 100
        assert abs(_compound_rate(pd.Series([1.0, 1.0])) - expected) < 1e-9

    def test_compound_exceeds_arithmetic_sum_for_multiple_periods(self):
        s = pd.Series([0.05] * 20)
        assert _compound_rate(s) > s.sum()

    def test_zero_rates_return_zero(self):
        assert _compound_rate(pd.Series([0.0, 0.0, 0.0])) == 0.0


class TestAggregateSelic:
    def test_monthly_output_has_required_columns(self, tmp_path, monkeypatch):
        df = _make_silver_df()
        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        aggregate_selic()

        monthly = pd.read_parquet(tmp_path / "gold" / "selic_mensal.parquet")
        for col in ("ano_mes", "media_diaria_mensal", "taxa_acumulada_mensal",
                    "variacao_mensal_pp", "dias_uteis", "data_inicio", "data_fim"):
            assert col in monthly.columns, f"Column '{col}' missing from selic_mensal"

    def test_annual_output_has_required_columns(self, tmp_path, monkeypatch):
        df = _make_silver_df()
        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        aggregate_selic()

        annual = pd.read_parquet(tmp_path / "gold" / "selic_anual.parquet")
        for col in ("ano", "media_diaria_anual", "taxa_acumulada_anual", "dias_uteis"):
            assert col in annual.columns, f"Column '{col}' missing from selic_anual"

    def test_first_month_variacao_is_nan(self, tmp_path, monkeypatch):
        df = _make_silver_df()
        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        aggregate_selic()

        monthly = pd.read_parquet(tmp_path / "gold" / "selic_mensal.parquet")
        assert pd.isna(monthly.loc[0, "variacao_mensal_pp"])

    def test_quality_gate_low_business_days(self, tmp_path, monkeypatch):
        """Month with < 10 business days must fail the quality gate."""
        dates = (
            pd.date_range("2020-01-02", periods=15, freq="B").tolist()
            + pd.date_range("2020-02-03", periods=5, freq="B").tolist()  # only 5 days in Feb
        )
        df = pd.DataFrame({"data": dates, "valor": [0.0267] * 20})
        df["ano"] = df["data"].dt.year
        df["mes"] = df["data"].dt.month
        df["ano_mes"] = df["data"].dt.to_period("M").astype(str)
        df["dia_semana"] = df["data"].dt.day_name()

        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        with pytest.raises(ValueError, match="quality gate"):
            aggregate_selic()

    def test_quality_gate_annual_rate_out_of_bounds(self, tmp_path, monkeypatch):
        """2% daily for a full year → annual compound > 25% → must fail."""
        dates = pd.date_range("2020-01-02", periods=252, freq="B")
        df = pd.DataFrame({"data": dates, "valor": [2.0] * 252})
        df["ano"] = df["data"].dt.year
        df["mes"] = df["data"].dt.month
        df["ano_mes"] = df["data"].dt.to_period("M").astype(str)
        df["dia_semana"] = df["data"].dt.day_name()

        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        with pytest.raises(ValueError, match="quality gate"):
            aggregate_selic()

    def test_monthly_output_sorted_by_date(self, tmp_path, monkeypatch):
        """Monthly output must be ordered by data_inicio regardless of groupby order."""
        df = _make_silver_df(months=2)
        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        aggregate_selic()

        monthly = pd.read_parquet(tmp_path / "gold" / "selic_mensal.parquet")
        assert monthly["data_inicio"].is_monotonic_increasing

    def test_is_base_month_true_only_for_first_row(self, tmp_path, monkeypatch):
        df = _make_silver_df(months=2)
        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        aggregate_selic()

        monthly = pd.read_parquet(tmp_path / "gold" / "selic_mensal.parquet")
        assert "is_base_month" in monthly.columns
        assert monthly["is_base_month"].sum() == 1
        assert monthly.loc[0, "is_base_month"] == True  # noqa: E712

    def test_returns_correct_row_counts(self, tmp_path, monkeypatch):
        df = _make_silver_df(months=2)
        monkeypatch.setattr("gold.aggregate.SILVER_PATH", _write_silver(tmp_path, df))
        monkeypatch.setattr("gold.aggregate.GOLD_PATH", tmp_path / "gold")

        result = aggregate_selic()

        assert result["monthly_rows"] == 2
        assert result["annual_rows"] == 1
