"""HTML report generation from comparison results using Jinja2 templates."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template

from .types import Comparison


class HtmlReportGenerator:
    """Generates HTML reports from database comparison results using Jinja2."""

    def __init__(self, template_dir: str | Path | None = None) -> None:
        """Initialize report generator with optional template directory."""
        if template_dir is None:
            # Use default templates directory relative to this file
            template_dir = Path(__file__).parent / "templates"

        self.template_dir = Path(template_dir)
        self.template_dir.mkdir(parents=True, exist_ok=True)

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

        def get_change_type(change: dict[str, object]) -> str:
            """Determine the type of change (added, removed, modified)."""
            if "new" in change and "old" not in change:
                return "added"
            elif "old" in change and "new" not in change:
                return "removed"
            else:
                return "modified"

        def count_changes(changes: list[object] | None) -> int:
            """Count the number of changes in a list."""
            return len(changes) if changes else 0

        def has_changes(table_comparison: dict[str, object]) -> bool:
            """Check if a table has any changes."""
            schema = table_comparison.get("schema")
            data = table_comparison.get("data")

            schema_count = count_changes(
                schema if isinstance(schema, list) or schema is None else None
            )
            data_count = count_changes(
                data if isinstance(data, list) or data is None else None
            )
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

        # Create default template if it doesn't exist
        template_path = self.template_dir / template_name
        if not template_path.exists():
            self._create_default_templates()

        # Load and render template
        template = self.env.get_template(template_name)
        html_content = template.render(comparison=result)

        # Write HTML file
        output_path.write_text(html_content, encoding="utf-8")

    def _create_default_templates(self) -> None:
        """Create default Jinja2 templates."""
        # Create base template
        self._create_base_template()

        # Create main comparison report template
        self._create_comparison_template()

        # Create CSS styles template
        self._create_styles_template()

        # Create JavaScript template
        self._create_scripts_template()

    def _create_base_template(self) -> None:
        """Create the base HTML template."""
        template_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Database Comparison Report{% endblock %}</title>
    {% include 'styles.html' %}
</head>
<body>
    <div class="container">
        {% block content %}{% endblock %}
    </div>
    {% include 'scripts.html' %}
</body>
</html>"""

        template_path = self.template_dir / "base.html"
        template_path.write_text(template_content, encoding="utf-8")

    def _create_comparison_template(self) -> None:
        """Create the main comparison report template."""
        template_content = """{% extends "base.html" %}

{% block content %}
<div class="header">
    <h1>Database Comparison Report</h1>
    <div class="metadata">
        <div><strong>Source:</strong> {{ comparison.source }}</div>
        <div><strong>Target:</strong> {{ comparison.target }}</div>
    </div>
</div>

<div class="summary">
    {% set total_tables = comparison.changes | length %}
    {% set tables_with_changes = [] %}
    {% for table in comparison.changes %}
        {% if table | has_changes %}
            {% set _ = tables_with_changes.append(table) %}
        {% endif %}
    {% endfor %}
    
    {% set total_schema_changes = 0 %}
    {% set total_data_changes = 0 %}
    {% for table in comparison.changes %}
        {% set total_schema_changes = total_schema_changes + (table.schema | count_changes) %}
        {% set total_data_changes = total_data_changes + (table.data | count_changes) %}
    {% endfor %}
    
    <div class="summary-card">
        <h3>Tables Compared</h3>
        <div class="number">{{ total_tables }}</div>
    </div>
    <div class="summary-card">
        <h3>Tables with Changes</h3>
        <div class="number">{{ tables_with_changes | length }}</div>
    </div>
    <div class="summary-card">
        <h3>Schema Changes</h3>
        <div class="number">{{ total_schema_changes }}</div>
    </div>
    <div class="summary-card">
        <h3>Data Changes</h3>
        <div class="number">{{ total_data_changes }}</div>
    </div>
</div>

<div class="section">
    <h2>Table Comparisons</h2>
    <div class="table-comparisons">
        {% for table in comparison.changes %}
            {% if table | has_changes %}
                {% include 'table_comparison.html' %}
            {% endif %}
        {% else %}
            <div class="no-changes">No changes detected</div>
        {% endfor %}
    </div>
</div>
{% endblock %}"""

        template_path = self.template_dir / "comparison_report.html"
        template_path.write_text(template_content, encoding="utf-8")

    def _create_table_template(self) -> None:
        """Create the table comparison template."""
        template_content = """<div class="table-comparison">
    {% set schema_count = table.schema | count_changes %}
    {% set data_count = table.data | count_changes %}
    {% set total_changes = schema_count + data_count %}
    
    <h3 class="expandable" onclick="toggleCollapsible(this)">
        {{ table.name }} ({{ total_changes }} changes)
    </h3>
    <div class="collapsible-content">
        {% if table.schema %}
            <h4>Schema Changes</h4>
            {% for change in table.schema %}
                <div class="change-item {{ change | get_change_type }}">
                    {{ change | get_change_type | upper }}: Column {{ change.name }}
                    <div class="details">
                        {% if change | get_change_type == 'added' %}
                            <strong>New column:</strong> {{ change.new | tojson }}
                        {% elif change | get_change_type == 'removed' %}
                            <strong>Removed column:</strong> {{ change.old | tojson }}
                        {% else %}
                            <strong>Modified column:</strong><br>
                            <span class="diff-old">Old: {{ change.old | tojson }}</span><br>
                            <span class="diff-new">New: {{ change.new | tojson }}</span>
                        {% endif %}
                    </div>
                </div>
            {% endfor %}
        {% endif %}

        {% if table.data %}
            <h4>Data Changes</h4>
            {% for change in table.data[:20] %}
                <div class="change-item {{ change | get_change_type }}">
                    <strong>{{ change | get_change_type | upper }}</strong> Row: 
                    {% for key, value in change.key.items() %}
                        {{ key }}={{ value | format_value }}{% if not loop.last %}, {% endif %}
                    {% endfor %}
                    <div class="details">
                        {% if change | get_change_type == 'added' %}
                            <strong>New values:</strong> {{ change.new | tojson }}
                        {% elif change | get_change_type == 'removed' %}
                            <strong>Removed values:</strong> {{ change.old | tojson }}
                        {% else %}
                            <strong>Modified values:</strong><br>
                            <span class="diff-old">Old: {{ change.old | tojson }}</span><br>
                            <span class="diff-new">New: {{ change.new | tojson }}</span>
                        {% endif %}
                    </div>
                </div>
            {% endfor %}
            
            {% if table.data | length > 20 %}
                <p><em>... and {{ table.data | length - 20 }} more data changes</em></p>
            {% endif %}
        {% endif %}
    </div>
</div>"""

        template_path = self.template_dir / "table_comparison.html"
        template_path.write_text(template_content, encoding="utf-8")

    def _create_styles_template(self) -> None:
        """Create the CSS styles template."""
        template_content = """<style>
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        line-height: 1.6;
        margin: 0;
        padding: 20px;
        background-color: #f5f5f5;
    }
    .container {
        max-width: 1200px;
        margin: 0 auto;
        background: white;
        padding: 30px;
        border-radius: 8px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    .header {
        border-bottom: 2px solid #e1e5e9;
        margin-bottom: 30px;
        padding-bottom: 20px;
    }
    .header h1 {
        color: #2c3e50;
        margin: 0 0 10px 0;
    }
    .metadata {
        color: #666;
        font-size: 14px;
    }
    .summary {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
    }
    .summary-card {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 6px;
        border-left: 4px solid #007bff;
    }
    .summary-card h3 {
        margin: 0 0 10px 0;
        color: #2c3e50;
    }
    .summary-card .number {
        font-size: 24px;
        font-weight: bold;
        color: #007bff;
    }
    .section {
        margin-bottom: 30px;
    }
    .section h2 {
        color: #2c3e50;
        border-bottom: 1px solid #e1e5e9;
        padding-bottom: 10px;
    }
    .table-comparison {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 6px;
        margin-bottom: 20px;
    }
    .change-item {
        background: white;
        margin: 10px 0;
        padding: 15px;
        border-radius: 4px;
        border-left: 4px solid #28a745;
    }
    .change-item.added {
        border-left-color: #28a745;
    }
    .change-item.removed {
        border-left-color: #dc3545;
    }
    .change-item.modified {
        border-left-color: #ffc107;
    }
    .details {
        margin-top: 10px;
        font-family: monospace;
        font-size: 12px;
        background: #f1f3f4;
        padding: 10px;
        border-radius: 4px;
    }
    .diff-old {
        color: #dc3545;
    }
    .diff-new {
        color: #28a745;
    }
    .expandable {
        cursor: pointer;
        user-select: none;
    }
    .expandable:hover {
        background-color: #e9ecef;
    }
    .expandable::before {
        content: "▶ ";
        display: inline-block;
        transition: transform 0.2s;
    }
    .expandable.expanded::before {
        transform: rotate(90deg);
    }
    .collapsible-content {
        display: none;
        margin-top: 15px;
    }
    .collapsible-content.show {
        display: block;
    }
    .no-changes {
        color: #666;
        font-style: italic;
        text-align: center;
        padding: 40px;
    }
</style>"""

        template_path = self.template_dir / "styles.html"
        template_path.write_text(template_content, encoding="utf-8")

    def _create_scripts_template(self) -> None:
        """Create the JavaScript template."""
        template_content = """<script>
    function toggleCollapsible(element) {
        const content = element.nextElementSibling;
        element.classList.toggle('expanded');
        content.classList.toggle('show');
    }

    // Auto-expand sections with changes on load
    document.addEventListener('DOMContentLoaded', function() {
        // Optionally auto-expand first few sections
        const expandables = document.querySelectorAll('.expandable');
        expandables.forEach((element, index) => {
            if (index < 3) { // Expand first 3 sections
                toggleCollapsible(element);
            }
        });
    });
</script>"""

        template_path = self.template_dir / "scripts.html"
        template_path.write_text(template_content, encoding="utf-8")

        # Also create the table comparison template
        self._create_table_template()
