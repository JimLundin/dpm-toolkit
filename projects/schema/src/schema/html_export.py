"""HTML export functionality for database schemas."""

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from schema.types import DatabaseSchema


def schema_to_html(schema: DatabaseSchema) -> str:
    """Create interactive HTML diagram from schema data."""
    # Get template directory
    template_dir = Path(__file__).parent / "templates"

    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
    template = env.get_template("diagram.html")

    title = f"ER Diagram - {schema['name']}"

    # Load CSS and JS content
    css_content = (template_dir / "diagram.css").read_text()
    js_content = (template_dir / "diagram.js").read_text()

    # Render template
    return template.render(
        title=title,
        css_content=css_content,
        js_content=js_content,
        diagram_json=json.dumps(schema, indent=2),
    )
