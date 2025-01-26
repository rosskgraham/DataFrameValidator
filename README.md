# DataFrameValidator

A proof of concept library to enable data validations for python dataframes.

The initial problem is the validation of pension scheme member data but this iibrary is intened to be a generic dataframe validation solution which could be imported into any notebook, script or pipeline regardless of the data domain.

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
(
    PolarsDataFrameValidator(customers)
    .expect_column_to_exist("Customer Id")
    .expect_column_to_contain_unique_values("Index")
    .expect_column_to_contain_unique_values("Customer Id")
    .expect_column_value_greater_than("Subscription Date", "1900-01-01")
    .expect_column_value_greater_than("Subscription Date", "2021-01-01", allow_nulls=True)
    .show_results()
)
┌───────────────────┬────────────────────────────────────────┬───────────────────────────────┬────────┬───────────┐
│ column_name       ┆ expectation_name                       ┆ expectation_args              ┆ result ┆ fail_rows │
╞═══════════════════╪════════════════════════════════════════╪═══════════════════════════════╪════════╪═══════════╡
│ Customer Id       ┆ expect_column_to_exist                 ┆                               ┆ true   ┆           │
│ Customer Id       ┆ expect_column_to_contain_unique_values ┆                               ┆ false  ┆ 2         │
│ Index             ┆ expect_column_to_contain_unique_values ┆                               ┆ false  ┆ 2         │
│ Subscription Date ┆ expect_column_value_greater_than       ┆ 1900-01-01, allow_nulls=False ┆ true   ┆ 0         │
│ Subscription Date ┆ expect_column_value_greater_than       ┆ 2021-01-01, allow_nulls=True  ┆ false  ┆ 427       │
└───────────────────┴────────────────────────────────────────┴───────────────────────────────┴────────┴───────────┘
```

## Possible Enhancements
- Redirect failing rows to quarantine for inspection
- Prettify the report output, add a UI
- 

## Observations
- 