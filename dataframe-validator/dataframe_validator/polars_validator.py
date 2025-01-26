import polars as pl
from typing import Self
from pydantic import BaseModel
from datetime import date, datetime


class ValidationResult(BaseModel):
    """Validation result for a single expectation"""

    column_name: str
    expectation_name: str
    expectation_args: str | None = ""
    result: bool
    fail_rows: int | None = None


class DataValidationError(Exception):
    pass


class PolarsDataFrameValidator:
    """Validator for Polars DataFrames
    
    Example usage:
    --------------
    >>> PolarsDataFrameValidator(customers) \\
        .expect_column_to_exist("Customer Id") \\
        .expect_column_to_contain_unique_values("Index") \\
        .show_results()
    ...┌───────────────────┬────────────────────────────────────────┬───────────────────────────────┬────────┬───────────┐
       │ column_name       ┆ expectation_name                       ┆ expectation_args              ┆ result ┆ fail_rows │
       ╞═══════════════════╪════════════════════════════════════════╪═══════════════════════════════╪════════╪═══════════╡
       │ Customer Id       ┆ expect_column_to_exist                 ┆                               ┆ true   ┆           │
       │ Customer Id       ┆ expect_column_to_contain_unique_values ┆                               ┆ false  ┆ 2         │
       │ Index             ┆ expect_column_to_contain_unique_values ┆                               ┆ false  ┆ 2         │
       │ Subscription Date ┆ expect_column_value_greater_than       ┆ 1900-01-01, allow_nulls=False ┆ true   ┆ 0         │
       │ Subscription Date ┆ expect_column_value_greater_than       ┆ 2021-01-01, allow_nulls=True  ┆ false  ┆ 427       │
       └───────────────────┴────────────────────────────────────────┴───────────────────────────────┴────────┴───────────┘
    """

    def __init__(
        self,
        df: pl.DataFrame,
        quarantine: bool = False, # Not implemented
    ):
        self.df = df
        self.validation_results: list[ValidationResult] = []
        self.quarantine = quarantine

    def expect_column_to_exist(
        self,
        column_name: str,
    ) -> Self:
        """Expect a column to exist in the DataFrame"""
        self.validation_results.append(
            ValidationResult(
                expectation_name="expect_column_to_exist",
                column_name=column_name,
                result=column_name in self.df.columns,
            )
        )
        return self

    def expect_column_to_contain_unique_values(
        self,
        column_name: str,
    ) -> Self:
        """Expect all values in a column to be unique"""
        validation_result = self.df.select(column_name).n_unique() == len(self.df)
        non_unique_rows = self.df.filter(self.df.select(column_name).is_unique().not_())

        self.validation_results.append(
            ValidationResult(
                expectation_name="expect_column_to_contain_unique_values",
                column_name=column_name,
                fail_rows=len(non_unique_rows),
                result=validation_result,
            )
        )
        return self

    def expect_column_value_greater_than(
        self,
        column_name: str,
        value: int | float | date | datetime,
        allow_nulls: bool = False,  # Not implemented
    ) -> Self:
        """Expect all values in a column to be greater than a given value"""
        validation_result = self.df[column_name].gt(value).all()
        fail_rows = self.df.filter(pl.col(column_name).le(value))

        self.validation_results.append(
            ValidationResult(
                expectation_name="expect_column_value_greater_than",
                expectation_args=f"{str(value)}, {allow_nulls=}",
                column_name=column_name,
                fail_rows=len(fail_rows),
                result=validation_result,
            )
        )
        return self

    def show_results(self):
        with pl.Config(
            tbl_hide_column_data_types=True,
            tbl_hide_dataframe_shape=True,
            fmt_str_lengths=1000,
        ):
            results = pl.DataFrame(
                [
                    result.model_dump()
                    for result in sorted(
                        self.validation_results, key=lambda x: x.column_name
                    )
                ]
            ).with_columns(pl.col("fail_rows").fill_null(""))
            print(results)
        return self

    def throw_error_if_invalid(self):
        """Throw an error if any validation results are False"""
        validation_failures = [
            result for result in self.validation_results if not result.result
        ]
        if validation_failures:
            err_msg = f"Validation failed for {len(validation_failures)} expectations"
            raise DataValidationError(err_msg)

    def expect_column_value_to_be_between(
        self, column_name: str, low_value, high_value
    ) -> Self:
        """Not implemented"""
        return self

    def expect_column_a_greater_than_column_b(
        self, column_a: str, column_b: str
    ) -> Self:
        """Not implemented"""
        return self

    def expect_column_value_to_match_regex(
        self, column_name: str, pattern: str
    ) -> Self:
        """Not implemented"""
        return self

    def expect_column_value_to_be_in_set(
        self, column_name: str, values: list[str]
    ) -> Self:
        """Not implemented"""
        return self

    def expect_column_to_be_of_type(self, column_name: str, column_type: type) -> Self:
        """Not implemented"""
        return self
    
    