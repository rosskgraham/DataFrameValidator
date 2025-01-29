from dataframe_validator.polars_validator import (
    PolarsDataFrameValidator,
    DataValidationError,
)
import polars as pl
from pathlib import Path

test_data_path = Path(__file__).parent
customers = pl.read_csv(test_data_path / "customers-1000.csv")


def validate_customer(
    customers: pl.DataFrame,
    show_results: bool = True,
    show_fails: bool = False,
    quarantine: bool = False,
    throw: bool = False,
) -> bool:
    """Returns True if the Customer DataFrame passes all validation checks, False otherwise"""
    validator = (
        PolarsDataFrameValidator(customers)
        .expect_column_to_exist("Customer Id")
        .expect_column_to_contain_unique_values("Index")
        .expect_column_to_contain_unique_values("Customer Id")
        .expect_column_value_greater_than("Subscription Date", "1900-01-01")
        .expect_column_value_greater_than(
            "Subscription Date", "2020-01-02", allow_nulls=True
        )
    )
    if show_results:
        validator.show_results()
    if show_fails:
        validator.show_failures()
    if quarantine:
        output_path = test_data_path / "fails-customers-1000.csv"
        validator.validation_fails.write_csv(output_path)
    if throw:
        validator.throw_error_if_invalid()

    return validator.is_valid


# Run the validator and see results
is_valid = validate_customer(customers)
print(f"Customers dataframe is{' ' if is_valid else ' not '}valid")

# Run the validator, see results and quarantine the failed rows
validate_customer(customers, quarantine=True)

# Run the validator and throw an error if invalid
try:
    validate_customer(
        customers, quarantine=True, throw=True, show_results=False, show_fails=False
    )
except DataValidationError as err:
    print("Caught the error")
    print(err)
    # Do something with the error, eg stop pipeline execution
