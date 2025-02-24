from dataframe_validator import (
    PolarsDataFrameValidator,
    DataValidationError,
)
import polars as pl
from pathlib import Path

customers = pl.read_csv(Path(__file__).parent / "pension_scheme_members.csv")

def validate_members(
    members: pl.DataFrame,
    show_results: bool = True,
    show_fails: bool = False,
    quarantine: bool = False,
    throw: bool = False,
) -> bool:
    """Returns True if the Members DataFrame passes all validation checks, False otherwise"""
    validator = (
        PolarsDataFrameValidator(members)
        .expect_column_to_exist("Member ID")
        .expect_column_to_contain_unique_values("Member ID")
        .expect_column_to_contain_unique_values("Nino")
        .expect_column_value_to_be_in_set("Status", ["Active", "Retired", "Deferred"])
    )
    if show_results:
        validator.show_results()
    if show_fails:
        validator.show_failures()
    if quarantine:
        output_path = Path(__file__).parent / "fail_pension_scheme_members.csv"
        validator.validation_fails.write_csv(output_path)
    if throw:
        validator.throw_error_if_invalid()

    return validator.is_valid


# Run the validator and see results
is_valid = validate_members(customers, show_results=True, show_fails=True, quarantine=True,)
print(f"Members dataframe is{' ' if is_valid else ' not '}valid")


# Run the validator, see results and quarantine the failed rows
validate_members(customers, quarantine=True)

# Run the validator and throw an error if invalid
try:
    validate_members(
        customers, quarantine=True, throw=True, show_results=False, show_fails=False
    )
except DataValidationError as err:
    print("Caught the error")
    print(err)
    # Do something with the error, eg stop pipeline execution
