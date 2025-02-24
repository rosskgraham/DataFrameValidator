from dataframe_validator import (
    ValidatorDataFrame,
)
import polars as pl
from pathlib import Path

df = pl.read_csv(Path(__file__).parent / "pension_scheme_members.csv")
#df = df.filter(pl.col("Status") == "Retired")#.select(pl.exclude("Surname"))

df = ValidatorDataFrame(df)

with pl.Config(tbl_cols=len(df.columns)):
    print(df.head(10))

(
    df
    .expect_column_to_exist("Member ID")
    .expect_column_to_exist("Gender")
    .expect_column_to_exist("Title")
    .expect_column_to_exist("Forename")
    .expect_column_to_exist("Surname")
    .expect_column_to_exist("Nino")
    .expect_column_to_exist("Date Of Birth")
    .expect_column_to_exist("Joining Date")
    .expect_column_to_contain_unique_values("Member ID")
    .expect_column_to_contain_unique_values("Nino")
    .expect_column_value_to_be_in_set("Status", ["Active", "Retired", "Deferred"])
    .expect_column_a_greater_than_column_b("Joining Date","Date Of Birth")
    .show_results()
    .show_failures()
)

