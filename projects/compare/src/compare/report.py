"""HTML report generation from comparison results using Jinja2 templates."""

from collections.abc import Collection
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from compare.types import (
    Change,
    ChangeType,
    Comparison,
    TableComparison,
)


class HtmlReportGenerator:
    """Generates HTML reports from database comparison results using Jinja2."""

    def __init__(self, template_dir: str | Path | None = None) -> None:
        """Initialize report generator with optional template directory."""
        if template_dir is None:
            # Use default templates directory relative to this file
            template_dir = Path(__file__).parent / "templates"

        self.template_dir = Path(template_dir)

        # Check if templates exist, if not, we'll use the existing ones
        if not self.template_dir.exists():
            msg = f"Template directory not found: {self.template_dir}"
            raise FileNotFoundError(msg)

        # Initialize Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Add custom filters
        self._add_custom_filters()

    def _add_custom_filters(self) -> None:
        """Add custom Jinja2 filters for report generation."""

        def get_change_type(change: dict[str, Change]) -> ChangeType:
            """Determine the type of change (added, removed, modified)."""
            if "new" in change and "old" not in change:
                return "added"
            if "old" in change and "new" not in change:
                return "removed"
            return "modified"

        def count_changes(changes: Collection[Change]) -> int:
            """Count the number of changes in a list."""
            return len(changes) or 0

        def has_changes(table_comparison: TableComparison) -> bool:
            """Check if a table has any changes."""
            schema = table_comparison["schema"]
            data = table_comparison["data"]

            schema_count = count_changes(schema)
            data_count = count_changes(data)
            return schema_count > 0 or data_count > 0

        def format_value(value: object) -> str:
            """Format a value for display."""
            if value is None:
                return "NULL"
            if isinstance(value, str):
                return f'"{value}"'
            return str(value)

        # Register filters
        self.env.filters["get_change_type"] = get_change_type
        self.env.filters["count_changes"] = count_changes
        self.env.filters["has_changes"] = has_changes
        self.env.filters["format_value"] = format_value

    def generate_report(
        self,
        result: Comparison,
        output_path: str | Path,
        template_name: str = "comparison_report.html",
    ) -> None:
        """Generate complete HTML report from comparison result."""
        output_path = Path(output_path)

        # Load and render template
        template = self.env.get_template(template_name)
        html_content = template.render(comparison=result)

        # Write HTML file
        output_path.write_text(html_content, encoding="utf-8")

    def list_available_templates(self) -> list[str]:
        """List all available template files in the template directory."""
        return [f.name for f in self.template_dir.glob("*.html")]
