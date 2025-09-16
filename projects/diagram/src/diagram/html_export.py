"""HTML export functionality for ER diagrams."""

import json
from pathlib import Path

from diagram.schema_types import DiagramSchema


def _remove_dom_content_loaded(js_content: str) -> str:
    """Remove the DOMContentLoaded event listener from app.js to avoid conflicts."""
    lines = js_content.split("\n")
    result_lines: list[str] = []
    skip_mode = False
    brace_count = 0

    for line in lines:
        if "document.addEventListener('DOMContentLoaded'" in line:
            skip_mode = True
            brace_count = 0
            result_lines.append("// Original DOMContentLoaded listener removed")
            continue

        if skip_mode:
            # Count braces to find the end of the event listener
            brace_count += line.count("{") - line.count("}")
            if brace_count <= 0 and "});" in line:
                skip_mode = False
                result_lines.append("// });")
                continue

        if not skip_mode:
            result_lines.append(line)

    return "\n".join(result_lines)


def create_standalone_html(diagram: DiagramSchema, title: str = "ER Diagram") -> str:
    """Create a standalone HTML file with embedded ER diagram."""
    # Read the webapp files
    webapp_dir = Path(__file__).parent.parent.parent / "webapp"

    try:
        css_content = (webapp_dir / "css" / "styles.css").read_text()
        diagram_js = (webapp_dir / "js" / "diagram.js").read_text()
        app_js = (webapp_dir / "js" / "app.js").read_text()
    except FileNotFoundError:
        # Fallback for minimal HTML if webapp files are not available
        return create_minimal_html(diagram, title)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
{css_content}
    </style>
</head>
<body>
    <div id="app">
        <header>
            <h1>{title}</h1>
            <div class="controls">
                <input type="file" id="jsonFileInput" accept=".json" style="display:none;" />
                <button id="resetViewBtn">Reset View</button>
                <button id="fitToScreenBtn">Fit to Screen</button>
                <div class="zoom-controls">
                    <button id="zoomInBtn">+</button>
                    <button id="zoomOutBtn">-</button>
                </div>
            </div>
        </header>

        <main>
            <div id="sidebar">
                <div class="info-panel">
                    <h3>Database Info</h3>
                    <div id="dbInfo">
                        <p>Loading database information...</p>
                    </div>
                </div>

                <div class="table-list">
                    <h3>Tables</h3>
                    <div id="tableList">
                        <p>Loading tables...</p>
                    </div>
                </div>

                <div class="legend">
                    <h3>Legend</h3>
                    <div class="legend-item">
                        <div class="legend-color primary-key"></div>
                        <span>Primary Key</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color foreign-key"></div>
                        <span>Foreign Key</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color unique-key"></div>
                        <span>Unique</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color nullable"></div>
                        <span>Nullable</span>
                    </div>
                </div>
            </div>

            <div id="diagram-container">
                <svg id="diagram"></svg>
                <div id="tooltip" class="tooltip hidden"></div>
            </div>
        </main>
    </div>

    <script>
// Embedded diagram data
const EMBEDDED_DIAGRAM_DATA = {json.dumps(diagram, indent=2)};

{diagram_js}

// App.js with DOMContentLoaded listener removed
{_remove_dom_content_loaded(app_js)}

// Safe event listener setup for embedded version
function safeAddEventListener(elementId, event, handler) {{
    const element = document.getElementById(elementId);
    if (element) {{
        element.addEventListener(event, handler);
    }} else {{
        console.warn(`Element with id '${{elementId}}' not found`);
    }}
}}

// Modified ERDiagramApp for embedded use
class EmbeddedERDiagramApp {{
    constructor() {{
        this.diagram = new ERDiagram('#diagram-container');
        this.currentData = null;
        this.selectedTable = null;
        this.initEventListeners();
    }}

    initEventListeners() {{
        // Control buttons - only add listeners if elements exist
        safeAddEventListener('resetViewBtn', 'click', () => {{
            this.diagram.resetView();
        }});

        safeAddEventListener('fitToScreenBtn', 'click', () => {{
            this.diagram.fitToScreen();
        }});

        safeAddEventListener('zoomInBtn', 'click', () => {{
            this.diagram.zoomIn();
        }});

        safeAddEventListener('zoomOutBtn', 'click', () => {{
            this.diagram.zoomOut();
        }});

        // Table selection
        window.addEventListener('tableSelected', (e) => {{
            this.handleTableSelection(e.detail);
        }});

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {{
            if (e.ctrlKey || e.metaKey) {{
                switch (e.key) {{
                    case '=':
                    case '+':
                        e.preventDefault();
                        this.diagram.zoomIn();
                        break;
                    case '-':
                        e.preventDefault();
                        this.diagram.zoomOut();
                        break;
                    case '0':
                        e.preventDefault();
                        this.diagram.resetView();
                        break;
                    case 'f':
                        e.preventDefault();
                        this.diagram.fitToScreen();
                        break;
                }}
            }}
        }});
    }}

    // Copy essential methods from original ERDiagramApp
    loadDiagramData(data) {{
        this.currentData = data;
        this.diagram.loadData(data);
        this.updateUI();
    }}

    updateUI() {{
        this.updateDatabaseInfo();
        this.updateTableList();
    }}

    updateDatabaseInfo() {{
        const dbInfo = document.getElementById('dbInfo');
        if (!dbInfo) return;

        // Count foreign keys across all tables
        const fkCount = this.currentData.tables.reduce((count, table) =>
            count + table.foreign_keys.length, 0);

        dbInfo.innerHTML = `
            <div class="info-item">
                <strong>Database:</strong> ${{this.currentData.name}}
            </div>
            <div class="info-item">
                <strong>Tables:</strong> ${{this.currentData.tables.length}}
            </div>
            <div class="info-item">
                <strong>Foreign Keys:</strong> ${{fkCount}}
            </div>
        `;
    }}

    updateTableList() {{
        const tableList = document.getElementById('tableList');
        if (!tableList) return;

        if (!this.currentData.tables.length) {{
            tableList.innerHTML = '<p>No tables found</p>';
            return;
        }}

        const tablesHtml = this.currentData.tables
            .sort((a, b) => a.name.localeCompare(b.name))
            .map(table => {{
                const columnCount = table.columns.length;
                const pkCount = table.primary_key.length;
                const fkCount = table.foreign_keys.length;
                return `
                    <div class="table-item" data-table="${{table.name}}">
                        <div class="table-name">${{table.name}}</div>
                        <div class="table-stats">
                            ${{columnCount}} cols • ${{pkCount}} PK • ${{fkCount}} FK
                        </div>
                    </div>
                `;
            }})
            .join('');

        tableList.innerHTML = tablesHtml;
    }}

    handleTableSelection(table) {{
        this.selectedTable = table;

        // Update table list selection
        document.querySelectorAll('.table-item').forEach(item => {{
            item.classList.toggle('selected', item.dataset.table === table.name);
        }});
    }}

    formatDate(dateString) {{
        if (!dateString) return 'Unknown';
        try {{
            const date = new Date(dateString);
            return date.toLocaleString();
        }} catch {{
            return dateString;
        }}
    }}
}}

// Auto-load the embedded data
document.addEventListener('DOMContentLoaded', () => {{
    try {{
        console.log('Initializing Embedded ER Diagram App...');
        window.app = new EmbeddedERDiagramApp();

        // Load the embedded diagram data
        if (EMBEDDED_DIAGRAM_DATA) {{
            console.log('Loading embedded diagram data:', EMBEDDED_DIAGRAM_DATA.name);
            window.app.loadDiagramData(EMBEDDED_DIAGRAM_DATA);
        }} else {{
            console.error('No embedded diagram data found');
        }}
    }} catch (error) {{
        console.error('Error initializing ER Diagram App:', error);
    }}
}});
    </script>
</body>
</html>"""


def create_minimal_html(diagram: DiagramSchema, title: str = "ER Diagram") -> str:
    """Create a minimal HTML file when webapp assets are not available."""
    tables = diagram["tables"]

    # Count total foreign keys
    total_fks = sum(len(table["foreign_keys"]) for table in tables)

    table_html = ""
    for table in tables:
        # Build columns HTML with PK/FK indicators
        columns_html = ""
        for col in table["columns"]:
            is_pk = col["name"] in table["primary_key"]
            is_fk = any(
                any(
                    mapping["source_column"] == col["name"]
                    for mapping in fk["column_mappings"]
                )
                for fk in table["foreign_keys"]
            )
            columns_html += f"<tr><td>{col['name']}</td><td>{col['type']}</td><td>{'✓' if is_pk else ''}</td><td>{'✓' if is_fk else ''}</td></tr>"

        table_html += f"""
        <div class="table-section">
            <h3>{table["name"]}</h3>
            <table>
                <thead>
                    <tr><th>Column</th><th>Type</th><th>PK</th><th>FK</th></tr>
                </thead>
                <tbody>
                    {columns_html}
                </tbody>
            </table>
        </div>
        """

    # Build relationships HTML from foreign keys
    relationships_html = ""
    for table in tables:
        for fk in table["foreign_keys"]:
            for mapping in fk["column_mappings"]:
                relationships_html += f"<tr><td>{table['name']}.{mapping['source_column']}</td><td>{fk['target_table']}.{mapping['target_column']}</td><td>many-to-one</td></tr>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .metadata {{ background: #f5f5f5; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}
        .table-section {{ margin-bottom: 30px; }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 10px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        h1, h2, h3 {{ color: #333; }}
        .relationships {{ margin-top: 40px; }}
    </style>
</head>
<body>
    <h1>{title}</h1>

    <div class="metadata">
        <h2>Database Information</h2>
        <p><strong>Database:</strong> {diagram["name"]}</p>
        <p><strong>Tables:</strong> {len(tables)}</p>
        <p><strong>Foreign Keys:</strong> {total_fks}</p>
    </div>

    <h2>Tables</h2>
    {table_html}

    <div class="relationships">
        <h2>Relationships</h2>
        <table>
            <thead>
                <tr><th>From</th><th>To</th><th>Type</th></tr>
            </thead>
            <tbody>
                {relationships_html}
            </tbody>
        </table>
    </div>

    <script>
        const diagramData = {json.dumps(diagram, indent=2)};
        console.log('Diagram data loaded:', diagramData);
    </script>
</body>
</html>"""
