# DPM2 Data Package

This package contains data artifacts for DPM databases, produced by the conversion logic in the OpenDPM project.

## Structure

- `dpm.sqlite`: SQLite database file
- `dpm.py`: SQLAlchemy models
- `utils.py`: Utility functions for working with the database

## Usage

This package is intended to be used as a dependency for applications that need access to DPM data models.

```python
from dpm2 import session, SomeTable

with session.begin() as s:
    # Use the session for database operations
    result = s.execute(select(SomeTable).where(SomeTable.some_column == some_value))
```

## Building the Data Package

The data artifacts in this package are generated during the release process using the conversion logic in the OpenDPM project. The conversion process:

1. Reads from source DPM databases
2. Transforms the data into standardized formats
3. Generates the necessary artifact files
4. Validates the output

To manually trigger the conversion process:

```bash
# From the root of the opendpm repository
python -m opendpm.convert.dpm2_builder
```

Note: The conversion process requires Windows with the proper dependencies installed (`convert` optional dependency group).
