import pandas as pd
import pytest

from silver.transform import _validate_bronze, transform_selic


def _write_bronze(tmp_path, df: pd.DataFrame):
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bronze_dir / "selic_raw.parquet", index=False)
    return bronze_dir


class TestValidateBronze:
    def test_raises_on_missing_valor_column(self):
        df = pd.DataFrame({"data": ["02/01/2020"]})
        with pytest.raises(ValueError, match="missing required columns"):
            _validate_bronze(df)

    def test_raises_on_missing_data_column(self):
        df = pd.DataFrame({"valor": ["0.0267"]})
        with pytest.raises(ValueError, match="missing required columns"):
            _validate_bronze(df)

    def test_passes_on_valid_schema(self, bronze_df):
        _validate_bronze(bronze_df)  # must not raise


class TestTransformSelic:
    def test_parses_date_and_valor_types(self, tmp_path, bronze_df, monkeypatch):
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, bronze_df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        transform_selic()

        result = pd.read_parquet(silver_dir / "selic_trusted.parquet")
        assert result["data"].dtype == "datetime64[ns]"
        assert result["valor"].dtype == "float64"

    def test_adds_all_derived_columns(self, tmp_path, bronze_df, monkeypatch):
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, bronze_df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        transform_selic()

        result = pd.read_parquet(silver_dir / "selic_trusted.parquet")
        for col in ("ano", "mes", "ano_mes", "dia_semana"):
            assert col in result.columns, f"Column '{col}' missing from Silver"

    def test_drops_rows_with_unparseable_valor(self, tmp_path, monkeypatch):
        df = pd.DataFrame({"data": ["02/01/2020", "03/01/2020"], "valor": ["0.0267", "N/A"]})
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        row_count = transform_selic()
        assert row_count == 1

    def test_output_is_sorted_ascending_by_date(self, tmp_path, monkeypatch):
        df = pd.DataFrame({
            "data": ["05/01/2020", "02/01/2020", "04/01/2020"],
            "valor": ["0.0267", "0.0265", "0.0266"],
        })
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        transform_selic()

        result = pd.read_parquet(silver_dir / "selic_trusted.parquet")
        assert result["data"].is_monotonic_increasing

    def test_quality_gate_valor_out_of_range(self, tmp_path, monkeypatch):
        df = pd.DataFrame({"data": ["02/01/2020"], "valor": ["99.9"]})
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        with pytest.raises(ValueError, match="quality gate"):
            transform_selic()

    def test_quality_gate_unexpected_year(self, tmp_path, monkeypatch):
        df = pd.DataFrame({"data": ["02/01/2019"], "valor": ["0.0267"]})
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        with pytest.raises(ValueError, match="unexpected years"):
            transform_selic()

    def test_quality_gate_duplicate_dates(self, tmp_path, monkeypatch):
        df = pd.DataFrame({
            "data": ["02/01/2020", "02/01/2020", "03/01/2020"],
            "valor": ["0.0267", "0.0267", "0.0267"],
        })
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        with pytest.raises(ValueError, match="duplicate dates"):
            transform_selic()

    def test_quality_gate_date_gap_too_large(self, tmp_path, monkeypatch):
        df = pd.DataFrame({
            "data": ["02/01/2020", "20/01/2020"],  # 18-day gap
            "valor": ["0.0267", "0.0267"],
        })
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        with pytest.raises(ValueError, match="gap of"):
            transform_selic()

    def test_passes_with_legitimate_holiday_gap(self, tmp_path, monkeypatch):
        """Christmas + New Year block (~10 days) must not trigger the gap gate."""
        df = pd.DataFrame({
            "data": ["24/12/2020", "04/01/2021"],  # 11-day gap, within threshold
            "valor": ["0.0267", "0.0267"],
        })
        silver_dir = tmp_path / "silver"
        monkeypatch.setattr("silver.transform.BRONZE_PATH", _write_bronze(tmp_path, df))
        monkeypatch.setattr("silver.transform.SILVER_PATH", silver_dir)

        transform_selic()  # must not raise
