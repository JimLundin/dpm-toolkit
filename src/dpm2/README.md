# DPM2 Data Package

This package contains data artifacts for DPM databases, produced by the conversion logic in the OpenDPM project.

## Structure

- `data/`: Contains the generated data artifacts
  - `models/`: Database models and schemas
  - `reference/`: Reference data and lookup tables
  - `metadata/`: Metadata about the DPM database structure

## Usage

This package is intended to be used as a dependency for applications that need access to DPM data models.

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
