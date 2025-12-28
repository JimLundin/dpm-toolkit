# Analysis Module Workflow

## Purpose

The analysis module discovers type refinement opportunities in **already-migrated SQLite databases**. It's a development tool that helps improve the migration heuristics in `projects/migrate/src/migrate/type_registry.py`.

## What to Analyze

Analyze **SQLite databases** that have been converted from Access databases. These are typically:
- Databases created by `dpm-toolkit migrate`
- Converted EBA DPM releases from your CI/CD pipeline
- Test databases used for development

## Integration with Existing Pipeline

### Current Pipeline (Before Analysis)

```
EBA Release (Access .accdb)
    ↓
dpm-toolkit download <version>
    ↓
dpm-toolkit migrate <file>.accdb --target <file>.sqlite
    ↓
dpm-toolkit schema <file>.sqlite --format python > models.py
```

### Enhanced Pipeline (With Analysis)

```
EBA Release (Access .accdb)
    ↓
dpm-toolkit download <version>
    ↓
dpm-toolkit migrate <file>.accdb --target <file>.sqlite
    ↓
dpm-toolkit analyze <file>.sqlite --format markdown > analysis.md  ← NEW
    ↓
[Manual Review of analysis.md]
    ↓
[Update type_registry.py based on findings]
    ↓
dpm-toolkit migrate <file>.accdb --target <file>-refined.sqlite
    ↓
dpm-toolkit schema <file>-refined.sqlite --format python > models.py
```

## Workflow Examples

### Example 1: Analyzing a Single Release

```bash
# 1. Download and migrate a release
dpm-toolkit download 4.1-final
dpm-toolkit migrate dpm-4.1-final.accdb --target dpm-4.1.sqlite

# 2. Analyze the migrated database
dpm-toolkit analyze dpm-4.1.sqlite --format markdown --output analysis-4.1.md

# 3. Review the analysis report
cat analysis-4.1.md

# 4. Look for high-confidence recommendations:
#    - Enum candidates with low cardinality
#    - Naming patterns (suffixes, prefixes)
#    - Boolean columns currently stored as integers

# 5. Update type_registry.py
#    Example: If analysis shows columns ending in "flag" are all boolean:
#
#    # In projects/migrate/src/migrate/type_registry.py
#    def column_type(col: Column) -> DataType | None:
#        col_name = col.name.lower()
#
#        # Add new pattern
#        if col_name.endswith("flag"):
#            return BOOLEAN
#
#        # ... existing code ...

# 6. Re-migrate with improved heuristics
dpm-toolkit migrate dpm-4.1-final.accdb --target dpm-4.1-refined.sqlite

# 7. Verify improvements by comparing
dpm-toolkit compare dpm-4.1.sqlite dpm-4.1-refined.sqlite --fmt html > diff.html
```

### Example 2: CI/CD Integration

Add analysis to your GitHub Actions workflow:

```yaml
- name: Migrate database
  run: dpm-toolkit migrate dpm-${{ matrix.version }}.accdb --target dpm-${{ matrix.version }}.sqlite

- name: Analyze for type refinements
  run: |
    dpm-toolkit analyze dpm-${{ matrix.version }}.sqlite \
      --format json \
      --output analysis-${{ matrix.version }}.json

- name: Upload analysis artifact
  uses: actions/upload-artifact@v6
  with:
    name: analysis-${{ matrix.version }}
    path: analysis-${{ matrix.version }}.json
```

Then periodically review artifacts to identify patterns across multiple releases.

### Example 3: Analyzing Multiple Releases

```bash
# Analyze several releases to find consistent patterns
for version in 4.0-final 4.1-final 4.2-final; do
    dpm-toolkit analyze dpm-${version}.sqlite \
        --format json \
        --output analysis-${version}.json
done

# Compare patterns across versions using jq
jq -s '
    map(.discovered_patterns[]) |
    group_by(.pattern) |
    map({
        pattern: .[0].pattern,
        occurrences_across_versions: length
    })
' analysis-*.json
```

## What the Analysis Reveals

### 1. Enum Candidates

**Current State:** Column stored as VARCHAR with many repeated values

**Analysis Output:**
```markdown
### ENUM Candidates

#### Taxonomy.TaxonomyVersion
- **Current Type:** VARCHAR(50)
- **Cardinality:** 3
- **Confidence:** 95.2%
- **Values:** 2.7, 3.0, 3.1
```

**Action:** Add to `enum_type()` function or update suffix patterns

### 2. Naming Patterns

**Current State:** Manual suffix list in `type_registry.py`

**Analysis Output:**
```markdown
### Suffix: `flag`
- **Type:** boolean
- **Occurrences:** 8
- **Confidence:** 87.5%
- **Examples:** ActiveFlag, DeletedFlag, ValidFlag

**Suggested code change:**
```python
if col_name.lower().endswith("flag"):
    return BOOLEAN
```

**Action:** Add discovered patterns to `column_type()` function

### 3. Date/Time Format Detection

**Current State:** Generic date casting in `value_casters.py`

**Analysis Output:**
```markdown
#### Taxonomy.ValidFrom
- **Current Type:** VARCHAR
- **Inferred Type:** date
- **Confidence:** 98.0%
- **Format:** date_iso
```

**Action:** Verify date formats are handled correctly

## Iterative Improvement Cycle

```
1. Migrate database with current heuristics
   ↓
2. Analyze to discover new patterns
   ↓
3. Update type_registry.py with high-confidence patterns
   ↓
4. Re-migrate to validate improvements
   ↓
5. Repeat with next release
```

## When to Run Analysis

- **After downloading a new EBA release** - Discover new columns/patterns
- **During development** - Test heuristic changes
- **Periodically across releases** - Identify consistent patterns
- **Before major refactoring** - Understand current type usage

## Output Artifacts

### JSON Format (Machine-Readable)
```bash
dpm-toolkit analyze database.sqlite --format json --output analysis.json
```

**Use cases:**
- Programmatic processing
- CI/CD integration
- Cross-version comparison
- Statistical aggregation

### Markdown Format (Human-Readable)
```bash
dpm-toolkit analyze database.sqlite --format markdown --output analysis.md
```

**Use cases:**
- Manual review
- Documentation
- Pull request comments
- Developer onboarding

## Configuration

### Confidence Threshold

Default: 0.7 (70% confidence)

```bash
# Only show high-confidence recommendations
dpm-toolkit analyze database.sqlite --confidence 0.9

# Show more recommendations (lower threshold)
dpm-toolkit analyze database.sqlite --confidence 0.5
```

### Tuning Inference Rules

Edit `projects/analysis/src/analysis/inference.py`:

```python
class TypeInferenceEngine:
    ENUM_CARDINALITY_THRESHOLD = 50  # Max unique values for enum
    ENUM_CARDINALITY_RATIO = 0.01    # Max ratio unique/total
    PATTERN_MATCH_THRESHOLD = 0.95   # Min pattern match %
```

## FAQ

**Q: Do I run this on Access databases?**
No. Analyze the **SQLite databases** that result from migration.

**Q: Does this modify my database?**
No. Analysis is read-only. It only generates reports.

**Q: How do I apply the recommendations?**
Manually review the report and update `type_registry.py`. Then re-migrate.

**Q: Should I analyze every database?**
No. Focus on:
- New releases with schema changes
- Representative samples across versions
- Databases showing migration issues

**Q: What if recommendations conflict with domain knowledge?**
Trust your domain knowledge. The analysis is a tool, not a mandate. Use it to discover patterns you might have missed.
