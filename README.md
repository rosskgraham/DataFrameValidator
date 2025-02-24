# DataFrameValidator

A proof of concept library to enable data validations for python dataframes.

The initial problem is the validation of pension scheme member data but this library is intended to be a generic dataframe validation solution which could be imported into any notebook, script or pipeline regardless of the data domain.

Focusing on polars DataFrames initially it could be extended to pandas as well.

## Getting Started

This project uses [uv](https://docs.astral.sh/uv/) to manage dependencies. If not already installed, follow the guide here, [Installing uv](https://docs.astral.sh/uv/getting-started/installation/#installing-uv).

Open a terminal at the path containing the ```pyproject.toml``` file and run ```uv sync```.

```powershell
PS ...\dataframe-validator> uv sync

```

## Run Tests
```powershell
PS ...\dataframe-validator> uv run pytest
```

## Build
```powershell
PS ...\dataframe-validator> uv build
```

## Run

Create an instance of a DataFrameValidator, passing a dataframe into the constructor.

Chain expectations onto the Validator then finally show results.

See ```validate\main.py``` for an example with test data.

```python
validator = (
    PolarsDataFrameValidator(members)
    .expect_column_to_exist("Member ID")
    .expect_column_to_contain_unique_values("Member ID")
    .expect_column_value_to_be_in_set("Status", ["Active", "Retired", "Deferred"])
    .show_results()
)
┌─────────────┬────────────────────────────────────────┬──────────────────────────────────────────┬────────┬───────────┐
│ column_name ┆ expectation_name                       ┆ expectation_args                         ┆ result ┆ fail_rows │
╞═════════════╪════════════════════════════════════════╪══════════════════════════════════════════╪════════╪═══════════╡
│ Member ID   ┆ expect_column_to_exist                 ┆                                          ┆ ✅     ┆           │
│ Member ID   ┆ expect_column_to_contain_unique_values ┆                                          ┆ ❌     ┆ 2         │
│ Status      ┆ expect_column_value_to_be_in_set       ┆ values=['Active', 'Retired', 'Deferred'] ┆ ❌     ┆ 1         │
└─────────────┴────────────────────────────────────────┴──────────────────────────────────────────┴────────┴───────────┘

# Print rows failing validation to the console
validator.show_failures()
┌────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────┬───────────┬────────┬─────┬──────────┐
│ expectation_name                       ┆ expectation_args                                                       ┆ Member ID ┆ Gender ┆ ... ┆ Status   │
╞════════════════════════════════════════╪════════════════════════════════════════════════════════════════════════╪═══════════╪════════╪═════╪══════════╡
│ expect_column_to_contain_unique_values ┆ {'column_name': 'Member ID'}                                           ┆ 123       ┆ Female ┆ ... ┆ Deferred │
│ expect_column_to_contain_unique_values ┆ {'column_name': 'Member ID'}                                           ┆ 123       ┆ Male   ┆ ... ┆ Deferred │
│ expect_column_value_to_be_in_set       ┆ {'column_name': 'Status', 'values': ['Active', 'Retired', 'Deferred']} ┆ 965       ┆ Male   ┆ ... ┆ Inactive │
└────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────┴───────────┴────────┴─────┴──────────┘

# Write failed rows to csv file for inspection
validator.validation_fails.write_csv(test_data_path / "fails-members.csv")

```

## Possible Enhancements
- Prettify the report output, add a UI
- Persist validation results so that changes in quality over time can be observed
- Autofix validation fails where possible and appropriate?

## Observations
- 