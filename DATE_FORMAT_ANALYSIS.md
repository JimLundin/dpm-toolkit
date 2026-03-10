# Date/DateTime Format Analysis Across DPM Database Versions

Analysis of all date and datetime formats found in the 11 converted SQLite databases
(versions 3.2-sample through 4.2-release).

## Summary

Only **two** date/datetime formats are used across all database versions:

| Format | Pattern | Column Type | Total Values |
|--------|---------|-------------|--------------|
| `YYYY-MM-DD` | `2024-12-19` | `DATE` | 180,130 |
| `YYYY-MM-DD HH:MM:SS.ffffff` | `2024-03-13 05:56:20.000000` | `DATETIME` | 4,294 |

Both are ISO 8601 compliant formats. No other date formats (slash-separated, dash-day-first,
T-separator, timezone-aware, etc.) were found in any database version.

## Columns Containing Date/DateTime Values

| Table | Column | SQLite Type | Format | Description |
|-------|--------|-------------|--------|-------------|
| `ModuleVersion` | `FromReferenceDate` | `DATE` | `YYYY-MM-DD` | Reference period start |
| `ModuleVersion` | `ToReferenceDate` | `DATE` | `YYYY-MM-DD` | Reference period end (nullable) |
| `OperationScope` | `FromSubmissionDate` | `DATE` | `YYYY-MM-DD` | Submission period start (nullable) |
| `Release` | `Date` | `DATE` | `YYYY-MM-DD` | Release date |
| `VariableGeneration` | `StartDate` | `DATETIME` | `YYYY-MM-DD HH:MM:SS.ffffff` | Generation start timestamp |
| `VariableGeneration` | `EndDate` | `DATETIME` | `YYYY-MM-DD HH:MM:SS.ffffff` | Generation end timestamp (nullable) |

## Per-Version Breakdown

### 3.2-sample

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 37 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 7,945 |
| `Release.Date` | `YYYY-MM-DD` | 1 |

*Note: `ToReferenceDate`, `StartDate`, and `EndDate` columns not yet present in this version.*

### 3.5-sample

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 42 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 4 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 7,723 |
| `Release.Date` | `YYYY-MM-DD` | 2 |

*Note: `ToReferenceDate` first appears here. `StartDate`/`EndDate` not yet present.*

### 4.0-draft

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 81 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 40 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 15,262 |
| `Release.Date` | `YYYY-MM-DD` | 3 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 134 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 133 |

*Note: `VariableGeneration.StartDate`/`EndDate` (DATETIME) first appear here.*

### 4.0-release

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 84 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 40 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 15,371 |
| `Release.Date` | `YYYY-MM-DD` | 3 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 169 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 168 |

### 4.0-errata-4

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 84 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 40 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 15,371 |
| `Release.Date` | `YYYY-MM-DD` | 3 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 169 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 168 |

*Identical to 4.0-release (errata did not change date values).*

### 4.0-errata-5

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 84 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 40 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 15,371 |
| `Release.Date` | `YYYY-MM-DD` | 3 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 169 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 168 |

*Identical to 4.0-release/errata-4.*

### 4.1-draft

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 96 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 39 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 16,712 |
| `Release.Date` | `YYYY-MM-DD` | 4 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 258 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 256 |

### 4.1-final

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 96 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 46 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 16,609 |
| `Release.Date` | `YYYY-MM-DD` | 4 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 288 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 286 |

### 4.1-errata-1

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 96 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 46 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 16,609 |
| `Release.Date` | `YYYY-MM-DD` | 4 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 288 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 286 |

*Identical to 4.1-final.*

### 4.2-draft

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 147 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 92 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 24,876 |
| `Release.Date` | `YYYY-MM-DD` | 5 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 323 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 321 |

### 4.2-release

| Column | Format | Count |
|--------|--------|-------|
| `ModuleVersion.FromReferenceDate` | `YYYY-MM-DD` | 147 |
| `ModuleVersion.ToReferenceDate` | `YYYY-MM-DD` | 94 |
| `OperationScope.FromSubmissionDate` | `YYYY-MM-DD` | 26,769 |
| `Release.Date` | `YYYY-MM-DD` | 5 |
| `VariableGeneration.StartDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 356 |
| `VariableGeneration.EndDate` | `YYYY-MM-DD HH:MM:SS.ffffff` | 354 |

## Key Observations

1. **Format consistency**: All 11 database versions use exactly the same two formats. There is zero format variation across versions.

2. **Schema evolution**:
   - `ModuleVersion.ToReferenceDate` (DATE) was added in **3.5-sample**
   - `VariableGeneration.StartDate` and `EndDate` (DATETIME) were added in **4.0-draft**
   - All other date columns exist from the earliest version (3.2-sample)

3. **The fallback date formats in `value_casters.py`** (`DD/MM/YYYY`, `MM/DD/YYYY`, `DD-MM-YYYY`, `YYYY/MM/DD`, and their datetime equivalents) are **never encountered** in actual data. All dates arrive in ISO format from the Access-to-SQLite conversion.

4. **Datetime microseconds**: All DATETIME values have `.000000` microseconds (6 zeros), indicating the source Access database stores timestamps at second precision only; the microsecond component is added during conversion.

5. **Data growth**: The number of rows in date columns grows steadily across versions, with `OperationScope.FromSubmissionDate` being by far the largest (7,945 rows in 3.2 to 26,769 in 4.2-release).
