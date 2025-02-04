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
    ):
        self.df = df
        self.validation_results: list[ValidationResult] = []
        self.validation_fails = pl.DataFrame()
        self._is_valid = True

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

        validation_fails = self.__add_validation_fail_columns(
            non_unique_rows,
            "expect_column_to_contain_unique_values",
            column_name=column_name,
        )

        self._is_valid = False if not validation_result else self._is_valid
        self.validation_fails = pl.concat([self.validation_fails, validation_fails])
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

        self._is_valid = False if not validation_result else self._is_valid
        validation_fails = self.__add_validation_fail_columns(
            fail_rows,
            "expect_column_value_greater_than",
            column_name=column_name,
            value=value,
            allow_nulls=allow_nulls,
        )

        self.validation_fails = pl.concat([self.validation_fails, validation_fails])

        self.validation_results.append(
            ValidationResult(
                expectation_name="expect_column_value_greater_than",
                expectation_args=f"{value=}, {allow_nulls=}",
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
            ).with_columns(
                pl.col("fail_rows").fill_null(""),
                pl.col("result").map_elements(
                    function=lambda x: "✅" if x else "❌", return_dtype=pl.Utf8
                ),
            )
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
        """Return True if all values in a column are in a given set"""
        validation_result = self.df[column_name].is_in(values).all()
        fail_rows = self.df.filter(pl.col(column_name).is_in(values).not_())

        self._is_valid = False if not validation_result else self._is_valid
        validation_fails = self.__add_validation_fail_columns(
            fail_rows,
            "expect_column_value_to_be_in_set",
            column_name=column_name,
            values=values,
        )

        self.validation_fails = pl.concat([self.validation_fails, validation_fails])

        self.validation_results.append(
            ValidationResult(
                expectation_name="expect_column_value_to_be_in_set",
                expectation_args=f"{values=}",
                column_name=column_name,
                fail_rows=len(fail_rows),
                result=validation_result,
            )
        )
        return self

    def expect_column_to_be_of_type(self, column_name: str, column_type: type) -> Self:
        """Not implemented"""
        return self

    def show_failures(self):
        with pl.Config(
            tbl_hide_column_data_types=True,
            tbl_hide_dataframe_shape=True,
            fmt_str_lengths=1000,
        ):
            print(self.validation_fails)
        return self

    def __add_validation_fail_columns(
        self, df: pl.DataFrame, expectation_name: str, **expectation_args
    ) -> pl.DataFrame:
        """Add expectation context columns to the failing rows."""
        df = df.insert_column(
            0,
            pl.lit(expectation_name).alias("expectation_name"),
        ).insert_column(
            1,
            pl.lit(f"{expectation_args}").alias("expectation_args"),
        )
        return df

    @property
    def is_valid(self):
        """Return True if all validation expectations are met"""
        return self._is_valid

    def expect_column_value_length_greater_than(
        self,
        column_name: str,
        length: int | float | date | datetime,
    ) -> Self:
        """Expect column values to be strings of length greater than a given value"""
        if self.df[column_name].dtype != pl.String:
            raise DataValidationError(f"Column '{column_name}' is not of string type")
        
        validation_result = self.df[column_name].str.len_chars().gt(length).all()
        fail_rows = self.df.filter(pl.col(column_name).str.len_chars().le(length))

        self._is_valid = False if not validation_result else self._is_valid
        validation_fails = self.__add_validation_fail_columns(
            fail_rows,
            "expect_column_value_greater_than",
            column_name=column_name,
            length=length,
        )

        self.validation_fails = pl.concat([self.validation_fails, validation_fails])

        self.validation_results.append(
            ValidationResult(
                expectation_name="expect_column_value_length_greater_than",
                expectation_args=f"{length=}",
                column_name=column_name,
                fail_rows=len(fail_rows),
                result=validation_result,
            )
        )
        return self