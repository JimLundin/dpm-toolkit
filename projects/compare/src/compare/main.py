"""Main database comparison functionality."""

import json
from pathlib import Path
from sqlite3 import Row

from jinja2 import Environment, FileSystemLoader

from compare.comparator import Comparator
from compare.inspector import Inspector
from compare.types import (
    ColMod,
    Comparison,
    RowMod,
    TableComparison,
)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def generate_report(result: str, output: Path) -> None:
    """Generate HTML report from comparison result."""
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("report.html")
    html_content = template.render(comparison_json=result)
    output.write_text(html_content, encoding="utf-8")


def encoder(obj: object) -> dict[str, str]:
    """Convert sqlite3.Row to a regular dict for JSON serialization."""
    if isinstance(obj, Row):
        return dict(obj)
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


def compare_databases(source_db: Path, target_db: Path) -> Comparison:
    """Compare two SQLite databases completely and return full results."""
    # Create inspectors
    source = Inspector(source_db)
    target = Inspector(target_db)

    # Create comparator
    comparator = Comparator(source, target)

    # Get all tables
    tables_added, tables_removed, tables_common = comparator.tables()

    # Compare common tables (schema and data)
    changes = [comparator.compare_table(table_name) for table_name in tables_common]

    # Add tables that were added (only exist in target)
    changes.extend(
        TableComparison(
            name=table_name,
            schema=(ColMod(new=col) for col in target.columns(table_name)),
            data=(RowMod(new=row) for row in target.data(table_name)),
        )
        for table_name in tables_added
    )

    # Add tables that were removed (only exist in source)
    changes.extend(
        TableComparison(
            name=table_name,
            schema=(ColMod(old=col) for col in source.columns(table_name)),
            data=(RowMod(old=row) for row in source.data(table_name)),
        )
        for table_name in tables_removed
    )

    # Build complete result
    return Comparison(source=str(source_db), target=str(target_db), changes=changes)


def comparison_to_json(result: Comparison, indent: int = 2) -> str:
    """Convert comparison result to JSON string."""
    return json.dumps(result, default=encoder, indent=indent, ensure_ascii=False)


def save_comparison_json(result: Comparison, output: Path) -> None:
    """Save comparison result as JSON file."""
    json_content = comparison_to_json(result)

    output.write_text(json_content, encoding="utf-8")


def load_comparison_json(json_path: Path) -> Comparison:
    """Load comparison result from JSON file."""
    content = json_path.read_text(encoding="utf-8")
    result: Comparison = json.loads(content)
    return result
