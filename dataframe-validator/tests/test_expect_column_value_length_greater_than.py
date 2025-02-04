import polars as pl
from dataframe_validator.polars_validator import PolarsDataFrameValidator, DataValidationError
import pytest

def test_expect_column_value_length_greater_than_zero_pass():
    df = pl.DataFrame(
        {"a": ["a", "b", "c"], "b": ["a", "aa", "aaa"], "c": ["one", "two", "three"]}
    )
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_length_greater_than("a", 0)
    assert validator.is_valid, (
        "Expected all values in column 'a' to be of length greater than 0"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 0, "Expected 0 validation failure"


def test_expect_column_value_length_greater_than_one_pass():
    df = pl.DataFrame(
        {"a": ["a", "b", "c"], "b": ["a", "aa", "aaa"], "c": ["one", "two", "three"]}
    )
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_length_greater_than("c", 1)
    assert validator.is_valid, (
        "Expected all values in column 'c' to be of length greater than 1"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 0, "Expected 0 validation failure"


def test_expect_column_value_length_greater_than_one_fail_one():
    df = pl.DataFrame(
        {"a": ["a", "b", "c"], "b": ["a", "aa", "aaa"], "c": ["one", "two", "three"]}
    )
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_length_greater_than("b", 1)
    assert not validator.is_valid, (
        "Expected all values in column 'b' to be of length greater than 1"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 1, "Expected 1 validation failure"


def test_expect_column_value_length_greater_than_one_fail_many():
    df = pl.DataFrame(
        {"a": ["a", "b", "c"], "b": ["a", "aa", "aaa"], "c": ["one", "two", "three"]}
    )
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_value_length_greater_than("b", 5)
    assert not validator.is_valid, (
        "Expected all values in column 'b' to be of length greater than 5"
    )
    assert len(validator.validation_results) == 1, "Expected 1 validation results"
    assert len(validator.validation_fails) == 3, "Expected 3 validation failure"

def test_expect_column_value_length_greater_than_fail_non_string_type():
    df = pl.DataFrame(
        {"a": [1,2,3]}
    )
    validator = PolarsDataFrameValidator(df)
    with pytest.raises(DataValidationError) as err:
        validator.expect_column_value_length_greater_than("a", 5)
    assert "Column 'a' is not of string type" in str(err.value)