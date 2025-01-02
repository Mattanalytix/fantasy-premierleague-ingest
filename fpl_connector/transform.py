import polars as pl


def transform_fixtures(fixtures: pl.DataFrame) -> pl.DataFrame:
    return fixtures.with_columns(
        pl.col("kickoff_time").str.to_datetime()
    )
