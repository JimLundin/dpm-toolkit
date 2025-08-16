"""HTML report generation from comparison results."""

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from compare.types import Comparison

TEMPLATE_DIR = Path(__file__).parent / "templates"


def generate_report(result: Comparison, output_path: str | Path) -> None:
    """Generate HTML report from comparison result."""
    json_data = json.dumps(result, separators=(",", ":"))
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("report.html")
    html_content = template.render(comparison_json=json_data)
    Path(output_path).write_text(html_content, encoding="utf-8")
