"""HTML export functionality for ER diagrams."""

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from diagram.schema_types import DiagramSchema


def diagram_to_html(diagram: DiagramSchema, title: str = "ER Diagram") -> str:
    """Create a simple interactive HTML diagram with movable nodes and arrows."""
    # Get template directory
    template_dir = Path(__file__).parent / "templates"

    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
    template = env.get_template("diagram.html")

    # Load CSS and JS content
    css_content = (template_dir / "diagram.css").read_text()
    js_content = (template_dir / "diagram.js").read_text()

    # Render template
    return template.render(
        title=title,
        css_content=css_content,
        js_content=js_content,
        diagram_json=json.dumps(diagram, indent=2),
    )
