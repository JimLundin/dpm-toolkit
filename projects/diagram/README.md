# DPM Diagram Generator

This subproject provides ER diagram generation and visualization functionality for DPM databases.

## Features

- **Database Introspection**: Automatically extracts table schemas and relationships from SQLite databases
- **JSON Export**: Generates clean, structured JSON representation of the database schema
- **HTML Export**: Creates interactive diagrams with movable table nodes and connecting arrows
- **TypedDict Schema**: Type-safe JSON structure with comprehensive validation

## API

### Core Functions

- `sqlite_to_diagram(engine: Engine) -> DiagramSchema`: Generate ER diagram from SQLite database
- `diagram_to_html(diagram: DiagramSchema, title: str) -> str`: Create interactive HTML visualization
- `read_only_sqlite(path: Path) -> Engine`: Create read-only SQLAlchemy engine

### CLI Integration

The diagram functionality is integrated into the main DPM Toolkit CLI:

```bash
# Generate JSON diagram
dpm-toolkit diagram database.sqlite > diagram.json

# Generate interactive HTML
dpm-toolkit diagram database.sqlite --output-format html > diagram.html
```

## JSON Schema

The generated JSON follows a clean, minimal structure:

```json
{
  "name": "database.sqlite",
  "tables": [
    {
      "name": "users",
      "columns": [
        {
          "name": "id",
          "type": "INTEGER",
          "nullable": false
        },
        {
          "name": "email",
          "type": "VARCHAR(255)",
          "nullable": false
        }
      ],
      "primary_keys": ["id"],
      "foreign_keys": []
    },
    {
      "name": "posts",
      "columns": [
        {
          "name": "id",
          "type": "INTEGER",
          "nullable": false
        },
        {
          "name": "user_id",
          "type": "INTEGER",
          "nullable": false
        }
      ],
      "primary_keys": ["id"],
      "foreign_keys": [
        {
          "target_table": "users",
          "column_mappings": [
            {
              "source_column": "user_id",
              "target_column": "id"
            }
          ]
        }
      ]
    }
  ]
}
```

## HTML Visualization

The HTML export creates a simple, interactive diagram with:

- **Movable Table Nodes**: Drag tables to reposition them
- **Connecting Arrows**: Visual representation of foreign key relationships
- **Clean Styling**: Professional look with table headers and column lists
- **Full-Screen Layout**: Uses entire viewport for maximum space
- **Template-Based**: Maintainable separation of HTML, CSS, and JavaScript

### Template Structure

```
src/diagram/templates/
├── diagram.html    # Main HTML template
├── diagram.css     # Styling
└── diagram.js      # Interactive functionality
```

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
- **Jinja2**: HTML template rendering
- **D3.js**: Interactive visualization (loaded via CDN)

## Architecture

- **Functional Design**: List comprehensions and pure functions for data processing
- **TypedDict Schemas**: Type-safe JSON structure validation
- **Template Separation**: Clean separation of HTML, CSS, and JavaScript
- **Minimal Schema**: No unnecessary metadata or redundant relationships
- **Memory Efficient**: Streaming approach suitable for large databases