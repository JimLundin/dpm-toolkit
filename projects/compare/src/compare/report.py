"""HTML report generation from comparison results using Jinja2 templates."""

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from compare.types import (
    Comparison,
)


class JsonHtmlReportGenerator:
    """Generates single-file HTML reports with embedded JSON and JavaScript rendering."""

    def __init__(self, template_dir: str | Path | None = None) -> None:
        """Initialize the JSON-based report generator with template directory."""
        if template_dir is None:
            # Use default templates directory relative to this file
            template_dir = Path(__file__).parent / "templates"

        self.template_dir = Path(template_dir)

        # Check if templates exist
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

    def generate_report(self, result: Comparison, output_path: str | Path) -> None:
        """Generate a single-file HTML report with embedded JSON and JavaScript rendering."""
        output_path = Path(output_path)

        # Convert comparison result to JSON
        json_data = json.dumps(result, indent=None, separators=(",", ":"))

        # Load and render template
        template = self.env.get_template("json_report.html")
        html_content = template.render(
            comparison=result,
            comparison_json=json_data,
        )

        # Write the single HTML file
        output_path.write_text(html_content, encoding="utf-8")
