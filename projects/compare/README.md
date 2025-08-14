# Compare

Database comparison functionality for DPM Toolkit.

## Usage

```bash
# Install
uv sync --extra compare

# Compare two databases
dpm-toolkit compare --source old.sqlite --target new.sqlite
```

## What it does

- **Schema comparison** (`schema.py`): Added/removed tables and columns
- **Data comparison** (`data.py`): Row count changes per table
- **Main interface** (`main.py`): Orchestrates both comparisons
- **Flexible**: Compare schema only, data only, or both

## Examples

```bash
# Compare everything
dpm-toolkit compare --source old.sqlite --target new.sqlite

# Schema changes only
dpm-toolkit compare --source old.sqlite --target new.sqlite --type schema

# Data changes only  
dpm-toolkit compare --source old.sqlite --target new.sqlite --type data
```