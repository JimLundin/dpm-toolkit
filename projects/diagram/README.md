# DPM Diagram Generator

This subproject provides ER diagram generation and visualization functionality for DPM databases.

## Features

- **Database Introspection**: Automatically extracts table schemas, relationships, and metadata from SQLite databases
- **JSON Export**: Generates structured JSON representation of the database schema
- **Interactive Visualization**: Static web application for viewing and manipulating ER diagrams
- **Force-Directed Layout**: Automatic positioning of tables using physics-based algorithms

## API

### Core Functions

- `sqlite_to_diagram_json(engine: Engine) -> str`: Generate ER diagram JSON from SQLite database
- `read_only_sqlite(path: Path) -> Engine`: Create read-only SQLAlchemy engine

### CLI Integration

The diagram functionality is integrated into the main DPM Toolkit CLI:

```bash
# Generate JSON diagram
dpm-toolkit diagram database.sqlite > diagram.json

# With explicit JSON output format
dpm-toolkit diagram database.sqlite --output-format json > diagram.json
```

## JSON Schema

The generated JSON follows a structured format with:

- **Metadata**: Database information, generation timestamp, table/relationship counts
- **Tables**: Complete table definitions with columns, types, constraints, and row counts
- **Relationships**: Foreign key relationships with cardinality information
- **Layout**: Viewport and positioning information for visualization

Example output structure:

```json
{
  "metadata": {
    "database_name": "sample.db",
    "generated_at": "2024-01-01T12:00:00",
    "table_count": 5,
    "relationship_count": 8
  },
  "tables": [
    {
      "name": "users",
      "columns": [
        {
          "name": "id",
          "type": "INTEGER",
          "nullable": false,
          "primary_key": true,
          "foreign_key": false
        }
      ],
      "primary_keys": ["id"],
      "row_count": 150
    }
  ],
  "relationships": [
    {
      "id": "1",
      "from_table": "posts",
      "from_column": "user_id",
      "to_table": "users",
      "to_column": "id",
      "relationship_type": "many-to-one"
    }
  ]
}
```

## Web Application

The `webapp/` directory contains a static web application for interactive ER diagram visualization:

### Features

- **File Loading**: Load JSON diagram files via file input or drag-and-drop
- **Interactive Visualization**: Pan, zoom, and manipulate the diagram
- **Table Selection**: Click tables for detailed information
- **Relationship Highlighting**: Hover over relationships to see details
- **Responsive Design**: Works on desktop and mobile devices

### Usage

1. Open `webapp/index.html` in a web browser
2. Load a JSON diagram file using the file input or drag-and-drop
3. Use the controls to navigate and explore the diagram:
   - **Pan**: Click and drag the diagram background
   - **Zoom**: Use zoom controls or mouse wheel
   - **Reset View**: Return to original viewport
   - **Fit to Screen**: Auto-scale to fit all tables

### Keyboard Shortcuts

- `Ctrl/Cmd + +`: Zoom in
- `Ctrl/Cmd + -`: Zoom out
- `Ctrl/Cmd + 0`: Reset view
- `Ctrl/Cmd + F`: Fit to screen

## Development

### Testing

```bash
# Run tests
uv run pytest projects/diagram/tests/

# Run with coverage
uv run pytest projects/diagram/tests/ --cov=diagram
```

### Code Quality

```bash
# Linting and formatting
ruff check projects/diagram/
ruff format projects/diagram/

# Type checking
mypy projects/diagram/src/
pyright projects/diagram/src/
```

## Dependencies

- **SQLAlchemy**: Database introspection and metadata extraction
- **D3.js**: Web-based visualization (loaded via CDN in webapp)

## Future Enhancements

- HTML export with embedded visualization
- More layout algorithms (hierarchical, circular)
- Table grouping and clustering
- Export to image formats (PNG, SVG)
- Custom styling and themes
- Database schema comparison visualization