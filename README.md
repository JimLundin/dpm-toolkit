# OpenDPM

Convert EBA DPM 2.0 (Data Point Model) databases from AccessDB to SQLite and DuckDB formats.

## Overview

This project provides pre-converted SQLite and DuckDB versions of the EBA DPM 2.0 databases, making them accessible across all platforms. The original AccessDB source is available at the [EBA DPM Website](https://www.eba.europa.eu/risk-and-data-analysis/reporting-frameworks/dpm-data-dictionary).

### Why This Project?

The current DPM 2.0 releases from EBA are only provided in AccessDB format, which has limited support outside of Windows operating systems. In addition, the DPM 2.0 releases are only provided in "incremental" format, keeping each release self-contained. This project solves these limitations by:
- Converting the databases to SQLite and DuckDB formats
- Combining all releases into a single file
- Making the data consistently accessible via HTTP
- Providing automated conversion through GitHub Actions
- Publishing new releases with converted databases

### Database Format Options

We provide two database format options with different purposes:

- **SQLite**: Preserves full compatibility with the Access database structure, maintaining all relationships, constraints, and referential integrity. This is the most complete representation of the original data and is recommended for applications that need the full database schema.

- **DuckDB**: A streamlined version optimized for analytical workloads without foreign keys, constraints, and indexes. This format is designed specifically for data analysis with faster query performance for analytical operations, which is the most common use case for these databases. Use this format if you primarily need to run analytical queries and don't rely on the relational structure.

## Getting Started

### Option 1: Download Pre-converted Databases

The easiest way to get started is to download the converted databases from our [Releases page](https://github.com/JimLundin/opendpm/releases). Each release contains the latest converted databases in both SQLite and DuckDB formats.

You can download the latest releases either by:
- Using these URLs in your scripts:
```
https://github.com/JimLundin/opendpm/releases/latest/download/dpm-sqlite.zip
https://github.com/JimLundin/opendpm/releases/latest/download/dpm-duckdb.zip
```
- Or clicking here: 
  - [Download Latest DPM SQLite Database](https://github.com/JimLundin/opendpm/releases/latest/download/dpm-sqlite.zip)
  - [Download Latest DPM DuckDB Database](https://github.com/JimLundin/opendpm/releases/latest/download/dpm-duckdb.zip)

### Option 2: Run the Conversion Yourself

If you want to run the conversion process yourself:

1. Fork this repository
2. Enable GitHub Actions in your fork
3. Run the conversion workflow manually
4. Find the converted databases in the workflow artifacts

## Development

If you want to run the conversion locally:

1. Clone the repository
2. Ensure you have Python 3.12+ installed
3. Install the Microsoft Access ODBC driver
4. Install the package in development mode: `pip install -e .`
5. Run the conversion with your preferred format:
   ```
   # Convert to SQLite (default)
   opendpm convert --input-dir <source_dir> --output-dir <target_dir>
   
   # Convert to DuckDB
   opendpm convert --input-dir <source_dir> --output-dir <target_dir> --format duckdb
   ```

## Performance Considerations

- **SQLite**: Maintains all relationships and constraints from the original Access database, providing a complete representation but may be slower for complex analytical queries.

- **DuckDB**: Offers significantly better performance for analytical queries and reduced data storage, optimized for OLAP operations. Conversion times may be slower, but query performance is substantially better for data analysis tasks.

## Caveats

- Currently only the current DPM release data is extracted, not data from previous releases
- Queries from the original database are not preserved
- Local conversion requires Microsoft Access ODBC driver to be installed

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

This is an unofficial tool and is not affiliated with or endorsed by the European Banking Authority (EBA).
