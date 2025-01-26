from dataframe_validator.polars_validator import PolarsDataFrameValidator
import polars as pl
from pathlib import Path

test_data_path = Path(__file__).parent / "customers-1000.csv"
customers = pl.read_csv(test_data_path)


(
    PolarsDataFrameValidator(customers)
    .expect_column_to_exist("Customer Id")
    .expect_column_to_contain_unique_values("Index")
    .expect_column_to_contain_unique_values("Customer Id")
    .expect_column_value_greater_than("Subscription Date", "1900-01-01")
    .expect_column_value_greater_than("Subscription Date", "2021-01-01", allow_nulls=True)
    .show_results()
    #.throw_error_if_invalid()
)
