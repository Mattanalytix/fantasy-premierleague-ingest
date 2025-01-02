import polars as pl


def dataframe_to_list(dataframe: pl.DataFrame) -> list:
    """
    convert dataframe to list of dictionaries
    @param dataframe polars dataframe
    @return list of dictionaries
    """
    return pl.Series(dataframe).to_list()
