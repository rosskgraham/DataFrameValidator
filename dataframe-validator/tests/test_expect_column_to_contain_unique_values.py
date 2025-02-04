import polars as pl
from dataframe_validator.polars_validator import PolarsDataFrameValidator
import pytest


@pytest.mark.parametrize(
    "data,column_name,result,failures",
    [
        (
            {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]},
            "a",
            True,
            0,
        ),
        (
            {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 8]},
            "b",
            True,
            0,
        ),
        (
            {"a": [1, 2, 3], "b": [4, 5, 6], "c": [8, 8, 8]},
            "c",
            False,
            3,
        ),
        (
            {"a": [1, 2, None], "b": [4, 5, 6], "c": [7, 8, 8]},
            "a",
            True,
            0,
        ),
        (
            {"a": [1, None, None], "b": [4, 5, 6], "c": [7, 8, 8]},
            "a",
            False,
            2,
        ),
    ],
)
def test_expect_column_to_contain_unique_values(data, column_name, result, failures):
    """Test column has unique values"""
    df = pl.DataFrame(data)
    validator = PolarsDataFrameValidator(df)
    validator.expect_column_to_contain_unique_values(column_name)
    
    assert validator.validation_results[0].column_name == column_name
    assert validator.validation_results[0].fail_rows == failures
    assert validator.validation_results[0].result is result
    assert len(validator.validation_fails) == failures, f"Expected {failures} validation failure rows"

