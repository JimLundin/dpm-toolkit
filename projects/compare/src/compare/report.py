"""HTML report generation from comparison results using Jinja2 templates."""

import json
import re
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

        def get_change_type_short(change: dict[str, Change]) -> str:
            """Get shortened change type for CSS classes (a/r/m)."""
            if "new" in change and "old" not in change:
                return "a"
            if "old" in change and "new" not in change:
                return "r"
            return "m"

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

        def format_cell_value(value: object) -> str:
            """Format a value for table cell display."""
            if value is None:
                return ""
            return str(value)

        def format_cell_with_tooltip(value: object) -> str:
            """Format a value with tooltip for truncated display."""
            if value is None:
                return ""
            str_value = str(value)
            if len(str_value) > 30:  # Show tooltip for long values
                return f'<span title="{str_value}">{str_value}</span>'
            return str_value

        def get_value_class(value: object) -> str:
            """Get CSS class for value type."""
            if value is None:
                return "null-value"
            if isinstance(value, str):
                return "string-value"
            if isinstance(value, int | float):
                return "number-value"
            if isinstance(value, bool):
                return "boolean-value"
            return ""

        def needs_tooltip(value: object, max_length: int = 35) -> bool:
            """Check if a value needs a tooltip (content longer than CSS truncation threshold).

            CSS max-width is 200px with 14px font. Being conservative with ~22 chars
            to account for varying character widths and ensure we only add tooltips
            when content will actually be truncated.
            """
            if value is None:
                return False
            str_value = str(value)
            return len(str_value) > max_length

        def format_table_value(value: object) -> tuple[str, str]:
            """Format a value for table display with CSS class."""
            if value is None:
                return "NULL", "null-value"
            if isinstance(value, str):
                return value, "string-value"
            if isinstance(value, int | float):
                return str(value), "number-value"
            if isinstance(value, bool):
                return str(value), "boolean-value"
            return str(value), ""

        # Register filters
        self.env.filters["get_change_type"] = get_change_type
        self.env.filters["get_change_type_short"] = get_change_type_short
        self.env.filters["count_changes"] = count_changes
        self.env.filters["has_changes"] = has_changes
        self.env.filters["format_value"] = format_value
        self.env.filters["format_cell_value"] = format_cell_value
        self.env.filters["format_cell_with_tooltip"] = format_cell_with_tooltip
        self.env.filters["get_value_class"] = get_value_class
        self.env.filters["needs_tooltip"] = needs_tooltip

    def _compress_html(self, html: str) -> str:
        """Minify HTML by removing all unnecessary whitespace and newlines.

        This aggressively compresses the HTML by removing indentation, newlines,
        and extra whitespace while preserving content and functionality.
        """
        # Remove all newlines and replace with spaces first
        html = re.sub(r"\n+", " ", html)

        # Remove leading/trailing whitespace
        html = html.strip()

        # Compress multiple spaces into single spaces
        html = re.sub(r"\s+", " ", html)

        # Remove whitespace between tags (aggressive minification)
        html = re.sub(r">\s+<", "><", html)

        # Remove whitespace around specific structural tags
        html = re.sub(
            r"\s*(</?(?:html|head|body|div|table|thead|tbody|tr|script|style)[^>]*>)\s*",
            r"\1",
            html,
        )

        # Remove whitespace around table cells and headers (but preserve content spaces)
        html = re.sub(r"\s*(</?(?:td|th)[^>]*>)\s*", r"\1", html)

        # Remove any remaining leading/trailing whitespace
        return html.strip()

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

        # Compress HTML to remove unnecessary whitespace
        compressed_html = self._compress_html(html_content)

        # Write HTML file
        output_path.write_text(compressed_html, encoding="utf-8")

    def list_available_templates(self) -> list[str]:
        """List all available template files in the template directory."""
        return [f.name for f in self.template_dir.glob("*.html")]


class JsonHtmlReportGenerator:
    """Generates single-file HTML reports with embedded JSON and JavaScript rendering."""

    def __init__(self) -> None:
        """Initialize the JSON-based report generator."""

    def _get_css_styles(self) -> str:
        """Get the CSS styles for the report."""
        return """
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        line-height: 1.6;
        margin: 0;
        padding: 20px;
        background-color: #f5f5f5;
    }
    .ct {
        max-width: 1600px;
        margin: 0 auto;
        background: white;
        padding: 30px;
        border-radius: 8px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    .h {
        border-bottom: 2px solid #e1e5e9;
        margin-bottom: 30px;
        padding-bottom: 20px;
    }
    .h h1 {
        color: #2c3e50;
        margin: 0 0 10px 0;
    }
    .md {
        color: #666;
        font-size: 14px;
    }
    .s {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
    }
    .sc {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 6px;
        border-left: 4px solid #007bff;
    }
    .sc h3 {
        margin: 0 0 10px 0;
        color: #2c3e50;
    }
    .sc .n {
        font-size: 24px;
        font-weight: bold;
        color: #007bff;
    }
    .se {
        margin-bottom: 30px;
    }
    .se h2 {
        color: #2c3e50;
        border-bottom: 1px solid #e1e5e9;
        padding-bottom: 10px;
    }
    .tc {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 6px;
        margin-bottom: 20px;
    }
    .tct {
        overflow-x: auto;
        margin: 15px 0;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        position: relative;
        max-height: 400px;
        overflow-y: auto;
    }
    .dt, .st {
        width: 100%;
        min-width: 600px;
        border-collapse: collapse;
        margin: 0;
        font-size: 14px;
    }
    .dt th, .st th {
        background: #f8f9fa;
        padding: 12px 16px;
        text-align: left;
        border: 1px solid #dee2e6;
        font-weight: 600;
        color: #495057;
        font-size: 13px;
        position: sticky;
        top: 0;
        z-index: 10;
    }
    .dt td, .st td {
        padding: 10px 16px;
        border: 1px solid #dee2e6;
        vertical-align: top;
        line-height: 1.4;
        max-width: 200px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        position: relative;
        cursor: help;
    }
    .dt tr:nth-child(even), .st tr:nth-child(even) {
        background: #f8f9fa;
    }
    .dt tr:hover, .st tr:hover {
        background: #e3f2fd;
    }
    .null {
        color: #9ca3af;
        font-style: italic;
        font-weight: 300;
    }
    .mc {
        background: #fff3cd !important;
        font-family: monospace;
        font-size: 12px;
        white-space: nowrap !important;
        min-width: 120px;
        max-width: none !important;
        overflow: visible !important;
        text-overflow: unset !important;
    }
    .ov {
        color: #dc3545;
        background: #f8d7da;
        padding: 1px 4px;
        border-radius: 2px;
        margin-right: 2px;
        font-size: 11px;
        display: inline-block;
        max-width: 80px;
        overflow: hidden;
        text-overflow: ellipsis;
        vertical-align: middle;
    }
    .nv {
        color: #155724;
        background: #d4edda;
        padding: 1px 4px;
        border-radius: 2px;
        margin-left: 2px;
        font-size: 11px;
        display: inline-block;
        max-width: 80px;
        overflow: hidden;
        text-overflow: ellipsis;
        vertical-align: middle;
    }
    .ar {
        color: #6c757d;
        font-weight: bold;
        margin: 0 1px;
        font-size: 10px;
        display: inline-block;
        vertical-align: middle;
    }
    .cb {
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
    }
    .cb.a { background: #d4edda; color: #155724; }
    .cb.r { background: #f8d7da; color: #721c24; }
    .cb.m { background: #fff3cd; color: #856404; }
    .ca { background: rgba(212, 237, 218, 0.3) !important; }
    .cr { background: rgba(248, 215, 218, 0.3) !important; }
    .cm { background: rgba(255, 243, 205, 0.3) !important; }
    .ex {
        cursor: pointer;
        user-select: none;
    }
    .ex:hover {
        background-color: #e9ecef;
    }
    .ex::before {
        content: "▶ ";
        display: inline-block;
        transition: transform 0.2s;
    }
    .ex.expanded::before {
        transform: rotate(90deg);
    }
    .cc {
        display: none;
        margin-top: 15px;
    }
    .cc.show {
        display: block;
    }
    .nc {
        color: #666;
        font-style: italic;
        text-align: center;
        padding: 40px;
    }
    .loading {
        color: #666;
        font-style: italic;
        text-align: center;
        padding: 20px;
    }
    .virtual-table {
        min-height: 200px;
    }
    .virtual-row {
        display: table-row;
    }
    .search-box {
        margin: 10px 0;
        padding: 8px 12px;
        border: 1px solid #dee2e6;
        border-radius: 4px;
        font-size: 14px;
        width: 300px;
    }
    h4 .cb {
        margin-left: 8px;
        margin-right: 4px;
        font-size: 11px;
        vertical-align: middle;
    }
        """

    def _get_javascript(self) -> str:
        """Get the JavaScript code for dynamic rendering."""
        return """
class DatabaseReportRenderer {
    constructor(comparisonData) {
        this.data = comparisonData;
        this.visibleRows = new Map(); // Track visible rows per table
        this.rowHeight = 35; // Approximate row height in pixels
        this.bufferSize = 10; // Extra rows to render outside viewport
        this.maxVisibleRows = 50; // Maximum rows to render at once
    }

    init() {
        this.renderSummary();
        this.renderTableList();
        console.log('Database comparison report loaded with dynamic rendering');
    }

    renderSummary() {
        const tables = this.data.changes;
        const totalTables = tables.length;
        const tablesWithChanges = tables.filter(t => t.schema.length > 0 || t.data.length > 0);
        const totalSchemaChanges = tables.reduce((sum, t) => sum + t.schema.length, 0);
        const totalDataChanges = tables.reduce((sum, t) => sum + t.data.length, 0);

        document.getElementById('total-tables').textContent = totalTables;
        document.getElementById('tables-with-changes').textContent = tablesWithChanges.length;
        document.getElementById('schema-changes').textContent = totalSchemaChanges;
        document.getElementById('data-changes').textContent = totalDataChanges;
    }

    renderTableList() {
        const container = document.getElementById('tables-container');
        const tables = this.data.changes.filter(t => t.schema.length > 0 || t.data.length > 0);

        if (tables.length === 0) {
            container.innerHTML = '<div class="nc">No changes detected</div>';
            return;
        }

        tables.forEach(table => {
            const tableElement = this.createTableSection(table);
            container.appendChild(tableElement);
        });
    }

    createTableSection(table) {
        const section = document.createElement('div');
        section.className = 'tc';

        const schemaCount = table.schema.length;
        const dataCount = table.data.length;
        const totalChanges = schemaCount + dataCount;

        section.innerHTML = `
            <h3 class="ex" onclick="window.renderer.toggleTable('${table.name}')">
                ${table.name} (${totalChanges} changes)
            </h3>
            <div class="cc" id="content-${table.name}">
                <div class="loading">Loading...</div>
            </div>
        `;

        return section;
    }

    toggleTable(tableName) {
        const header = document.querySelector(`h3[onclick*="'${tableName}'"]`);
        const content = document.getElementById(`content-${tableName}`);

        header.classList.toggle('expanded');
        content.classList.toggle('show');

        if (content.classList.contains('show') && content.innerHTML.includes('Loading...')) {
            this.loadTableContent(tableName);
        }
    }

    loadTableContent(tableName) {
        const table = this.data.changes.find(t => t.name === tableName);
        const content = document.getElementById(`content-${tableName}`);

        let html = '';

        // Schema changes
        if (table.schema.length > 0) {
            html += this.renderSchemaSection(table);
        }

        // Data changes with virtual scrolling
        if (table.data.length > 0) {
            html += this.renderDataSection(table);
        }

        content.innerHTML = html;

        // Initialize virtual scrolling for data tables
        if (table.data.length > this.maxVisibleRows) {
            this.initVirtualScrolling(tableName, table.data);
        }
    }

    renderSchemaSection(table) {
        const changes = table.schema;
        const counters = this.countChangeTypes(changes);

        let html = `
            <h4>Schema Changes
                ${counters.added > 0 ? `<span class="cb a">Added</span> ${counters.added}` : ''}
                ${counters.removed > 0 ? `<span class="cb r">Removed</span> ${counters.removed}` : ''}
                ${counters.modified > 0 ? `<span class="cb m">Modified</span> ${counters.modified}` : ''}
            </h4>
            <div class="tct">
                <table class="st">
                    <thead>
                        <tr>
                            <th>Column</th>
                            <th>Old Type</th>
                            <th>New Type</th>
                            <th>Old Nullable</th>
                            <th>New Nullable</th>
                            <th>Old Default</th>
                            <th>New Default</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        changes.forEach(change => {
            html += this.renderSchemaRow(change);
        });

        html += `
                    </tbody>
                </table>
            </div>
        `;

        return html;
    }

    renderSchemaRow(change) {
        const changeType = this.getChangeType(change);
        const changeClass = changeType === 'added' ? 'ca' : changeType === 'removed' ? 'cr' : 'cm';

        let html = `<tr class="${changeClass}">`;
        html += `<td title="${change.name}">${change.name}</td>`;

        if (changeType === 'added') {
            html += `<td>-</td>`;
            html += `<td title="${change.new.type}">${change.new.type}</td>`;
            html += `<td>-</td>`;
            html += `<td>${change.new.nullable}</td>`;
            html += `<td>-</td>`;
            html += `<td>${this.formatValue(change.new.default)}</td>`;
        } else if (changeType === 'removed') {
            html += `<td title="${change.old.type}">${change.old.type}</td>`;
            html += `<td>-</td>`;
            html += `<td>${change.old.nullable}</td>`;
            html += `<td>-</td>`;
            html += `<td>${this.formatValue(change.old.default)}</td>`;
            html += `<td>-</td>`;
        } else {
            html += `<td title="${change.old.type}">${change.old.type}</td>`;
            html += `<td title="${change.new.type}">${change.new.type}</td>`;
            html += `<td>${change.old.nullable}</td>`;
            html += `<td>${change.new.nullable}</td>`;
            html += `<td>${this.formatValue(change.old.default)}</td>`;
            html += `<td>${this.formatValue(change.new.default)}</td>`;
        }

        html += `</tr>`;
        return html;
    }

    renderDataSection(table) {
        const changes = table.data;
        const counters = this.countChangeTypes(changes);
        const allColumns = this.getAllColumns(changes);

        let html = `
            <h4>Data Changes
                ${counters.added > 0 ? `<span class="cb a">Added</span> ${counters.added}` : ''}
                ${counters.removed > 0 ? `<span class="cb r">Removed</span> ${counters.removed}` : ''}
                ${counters.modified > 0 ? `<span class="cb m">Modified</span> ${counters.modified}` : ''}
            </h4>
        `;

        if (changes.length > this.maxVisibleRows) {
            html += `<div class="search-box-container">
                <input type="text" class="search-box" placeholder="Search data changes..."
                       onkeyup="window.renderer.filterDataRows('${table.name}', this.value)">
            </div>`;
        }

        html += `
            <div class="tct">
                <table class="dt" id="data-table-${table.name}">
                    <thead>
                        <tr>
                            ${allColumns.map(col => `<th>${col}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody id="data-tbody-${table.name}" class="virtual-table">
                    </tbody>
                </table>
            </div>
        `;

        return html;
    }

    initVirtualScrolling(tableName, dataChanges) {
        const tbody = document.getElementById(`data-tbody-${tableName}`);
        const container = tbody.closest('.tct');
        const allColumns = this.getAllColumns(dataChanges);

        // Set initial height
        tbody.style.height = `${dataChanges.length * this.rowHeight}px`;

        // Render initial visible rows
        this.renderVisibleRows(tableName, dataChanges, allColumns, 0, this.maxVisibleRows);

        // Add scroll listener
        container.addEventListener('scroll', () => {
            this.handleScroll(tableName, dataChanges, allColumns, container);
        });
    }

    handleScroll(tableName, dataChanges, allColumns, container) {
        const scrollTop = container.scrollTop;
        const startIndex = Math.floor(scrollTop / this.rowHeight);
        const endIndex = Math.min(startIndex + this.maxVisibleRows, dataChanges.length);

        this.renderVisibleRows(tableName, dataChanges, allColumns, startIndex, endIndex);
    }

    renderVisibleRows(tableName, dataChanges, allColumns, startIndex, endIndex) {
        const tbody = document.getElementById(`data-tbody-${tableName}`);

        // Clear existing rows
        tbody.innerHTML = '';

        // Add spacer for scrolled content above
        if (startIndex > 0) {
            const spacer = document.createElement('tr');
            spacer.style.height = `${startIndex * this.rowHeight}px`;
            spacer.innerHTML = `<td colspan="${allColumns.length}"></td>`;
            tbody.appendChild(spacer);
        }

        // Render visible rows
        for (let i = startIndex; i < endIndex; i++) {
            const change = dataChanges[i];
            const row = document.createElement('tr');
            row.className = 'virtual-row ' + (this.getChangeType(change) === 'added' ? 'ca' :
                           this.getChangeType(change) === 'removed' ? 'cr' : 'cm');
            row.innerHTML = this.renderDataRowContent(change, allColumns);
            tbody.appendChild(row);
        }

        // Add spacer for content below
        if (endIndex < dataChanges.length) {
            const spacer = document.createElement('tr');
            spacer.style.height = `${(dataChanges.length - endIndex) * this.rowHeight}px`;
            spacer.innerHTML = `<td colspan="${allColumns.length}"></td>`;
            tbody.appendChild(spacer);
        }
    }

    renderDataRowContent(change, allColumns) {
        const changeType = this.getChangeType(change);
        let html = '';

        allColumns.forEach(col => {
            if (changeType === 'added') {
                const val = change.new?.[col];
                html += `<td title="${val || 'NULL'}">${this.formatValue(val)}</td>`;
            } else if (changeType === 'removed') {
                const val = change.old?.[col];
                html += `<td title="${val || 'NULL'}">${this.formatValue(val)}</td>`;
            } else {
                const oldVal = change.old?.[col];
                const newVal = change.new?.[col];
                if (oldVal !== newVal) {
                    html += `<td class="mc">
                        <span class="ov" title="${oldVal || 'NULL'}">${this.formatValue(oldVal)}</span>
                        <span class="ar">→</span>
                        <span class="nv" title="${newVal || 'NULL'}">${this.formatValue(newVal)}</span>
                    </td>`;
                } else {
                    html += `<td title="${newVal || 'NULL'}">${this.formatValue(newVal)}</td>`;
                }
            }
        });

        return html;
    }

    filterDataRows(tableName, searchTerm) {
        // Implementation for search filtering would go here
        // For now, we'll keep it simple and not implement filtering
        console.log(`Filtering ${tableName} with term: ${searchTerm}`);
    }

    getAllColumns(changes) {
        const columns = new Set();
        changes.forEach(change => {
            if (change.new) Object.keys(change.new).forEach(col => columns.add(col));
            if (change.old) Object.keys(change.old).forEach(col => columns.add(col));
        });
        return Array.from(columns);
    }

    countChangeTypes(changes) {
        return changes.reduce((counts, change) => {
            const type = this.getChangeType(change);
            counts[type]++;
            return counts;
        }, { added: 0, removed: 0, modified: 0 });
    }

    getChangeType(change) {
        if (change.new && !change.old) return 'added';
        if (change.old && !change.new) return 'removed';
        return 'modified';
    }

    formatValue(value) {
        if (value === null || value === undefined) return 'NULL';
        if (value === '') return '';
        return String(value);
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    window.renderer = new DatabaseReportRenderer(window.comparisonData);
    window.renderer.init();
});
        """

    def generate_report(self, result: Comparison, output_path: str | Path) -> None:
        """Generate a single-file HTML report with embedded JSON and JavaScript rendering."""
        output_path = Path(output_path)

        # Convert comparison result to JSON
        json_data = json.dumps(result, indent=None, separators=(",", ":"))

        # Create complete HTML with embedded CSS, JS, and data
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Comparison Report</title>
    <style>{self._get_css_styles()}</style>
</head>
<body>
    <div class="ct">
        <div class="h">
            <h1>Database Comparison Report</h1>
            <div class="md">
                <div><strong>Source:</strong> {result["source"]}</div>
                <div><strong>Target:</strong> {result["target"]}</div>
            </div>
        </div>

        <div class="s">
            <div class="sc">
                <h3>Tables Compared</h3>
                <div class="n" id="total-tables">-</div>
            </div>
            <div class="sc">
                <h3>Tables with Changes</h3>
                <div class="n" id="tables-with-changes">-</div>
            </div>
            <div class="sc">
                <h3>Schema Changes</h3>
                <div class="n" id="schema-changes">-</div>
            </div>
            <div class="sc">
                <h3>Data Changes</h3>
                <div class="n" id="data-changes">-</div>
            </div>
        </div>

        <div class="se">
            <h2>Table Comparisons</h2>
            <div class="tcs" id="tables-container">
                <div class="loading">Loading tables...</div>
            </div>
        </div>
    </div>

    <script>
        window.comparisonData = {json_data};
        {self._get_javascript()}
    </script>
</body>
</html>"""

        # Write the single HTML file
        output_path.write_text(html_content, encoding="utf-8")
