import polars as pl
from dataframe_validator.polars_validator import PolarsDataFrameValidator

def test_expect_column_to_exist_pass_result_count():
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9]
    })
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_to_exist("a").expect_column_to_exist("b").expect_column_to_exist("c")
    assert len(validator.validation_results) == 3, "Expected 3 validation results"

def test_expect_column_to_exist_pass_result():
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9]
    })
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_to_exist("a").expect_column_to_exist("b").expect_column_to_exist("c")
    assert all([result.result for result in validator.validation_results]), "Expected all three columns to exist"

def test_expect_column_to_exist_fail_result():
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
    })
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_to_exist("a").expect_column_to_exist("b").expect_column_to_exist("c")
    assert len(validator.validation_results) == 3, "Expected 3 validation results"
    assert len([result.result for result in validator.validation_results if result.result]) == 2, "Expected all three columns to exist"
    assert len([result.result for result in validator.validation_results if not result.result]) == 1, "Expected all three columns to exist"