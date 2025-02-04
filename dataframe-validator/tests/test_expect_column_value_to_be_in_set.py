import polars as pl
from dataframe_validator.polars_validator import PolarsDataFrameValidator


def test_expect_column_value_to_be_in_set_pass_int_type():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_to_be_in_set("a", [1, 2, 3])
    assert validator.is_valid, (
        "Expected all values in int column 'a' to be in the set [1,2,3]"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 0, "Expected 0 validation failure"


def test_expect_column_value_to_be_in_set_pass_str_type():
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["4", "5", "6"], "c": [7, 8, 9]})
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_to_be_in_set("b", ["4", "5", "6"])
    assert validator.is_valid, (
        "Expected all values in string column 'b' to be in the set ['4', '5', '6']"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 0, "Expected 0 validation failure"


def test_expect_column_value_to_be_in_set_fail_one():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_to_be_in_set("c", [8, 9, 0])
    assert not validator.is_valid, (
        "Expected all values in int column 'c' to be in the set [8, 9, 0]"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 1, "Expected 1 validation failure"


def test_expect_column_value_to_be_in_set_fail_many():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_to_be_in_set("c", [1, 2, 3])
    assert not validator.is_valid, (
        "Expected all values in int column 'c' to be in the set [1, 2, 3]"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 3, "Expected 3 validation failure"
