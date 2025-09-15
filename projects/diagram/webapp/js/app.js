/**
 * Main application logic for the ER Diagram Viewer
 */

class ERDiagramApp {
    constructor() {
        this.diagram = new ERDiagram('#diagram-container');
        this.currentData = null;
        this.selectedTable = null;

        this.initEventListeners();
    }

    initEventListeners() {
        // File input
        document.getElementById('jsonFileInput').addEventListener('change', (e) => {
            this.handleFileLoad(e);
        });

        // Control buttons
        document.getElementById('resetViewBtn').addEventListener('click', () => {
            this.diagram.resetView();
        });

        document.getElementById('fitToScreenBtn').addEventListener('click', () => {
            this.diagram.fitToScreen();
        });

        document.getElementById('zoomInBtn').addEventListener('click', () => {
            this.diagram.zoomIn();
        });

        document.getElementById('zoomOutBtn').addEventListener('click', () => {
            this.diagram.zoomOut();
        });

        // Table selection
        window.addEventListener('tableSelected', (e) => {
            this.handleTableSelection(e.detail);
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.ctrlKey || e.metaKey) {
                switch (e.key) {
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
                }
            }
        });

        // Drag and drop for JSON files
        const dropZone = document.getElementById('diagram-container');

        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.style.backgroundColor = '#f0f8ff';
        });

        dropZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            dropZone.style.backgroundColor = '';
        });

        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.style.backgroundColor = '';

            const files = e.dataTransfer.files;
            if (files.length > 0 && files[0].type === 'application/json') {
                this.loadJsonFile(files[0]);
            }
        });
    }

    handleFileLoad(event) {
        const file = event.target.files[0];
        if (file) {
            this.loadJsonFile(file);
        }
    }

    loadJsonFile(file) {
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const data = JSON.parse(e.target.result);
                this.loadDiagramData(data);
            } catch (error) {
                this.showError('Invalid JSON file: ' + error.message);
            }
        };
        reader.readAsText(file);
    }

    loadDiagramData(data) {
        // Validate the data structure
        if (!this.validateData(data)) {
            this.showError('Invalid diagram data format');
            return;
        }

        this.currentData = data;
        this.diagram.loadData(data);
        this.updateUI();
    }

    validateData(data) {
        return data &&
               data.database_name &&
               Array.isArray(data.tables);
    }

    updateUI() {
        this.updateDatabaseInfo();
        this.updateTableList();
    }

    updateDatabaseInfo() {
        const dbInfo = document.getElementById('dbInfo');
        if (!dbInfo) return;

        // Count foreign keys across all tables
        const fkCount = this.currentData.tables.reduce((count, table) =>
            count + table.foreign_keys.length, 0);

        dbInfo.innerHTML = `
            <div class="info-item">
                <strong>Database:</strong> ${this.currentData.database_name || 'Unknown'}
            </div>
            <div class="info-item">
                <strong>Tables:</strong> ${this.currentData.tables.length}
            </div>
            <div class="info-item">
                <strong>Foreign Keys:</strong> ${fkCount}
            </div>
        `;
    }

    updateTableList() {
        const tableList = document.getElementById('tableList');

        if (!this.currentData.tables.length) {
            tableList.innerHTML = '<p>No tables found</p>';
            return;
        }

        const tablesHtml = this.currentData.tables
            .sort((a, b) => a.name.localeCompare(b.name))
            .map(table => {
                const columnCount = table.columns.length;
                const pkCount = table.primary_key ? table.primary_key.length : 0;
                const fkCount = table.foreign_keys ? table.foreign_keys.length : 0;

                return `
                    <div class="table-item" data-table="${table.name}">
                        <div class="table-name">${table.name}</div>
                        <div class="table-stats">
                            ${columnCount} cols • ${pkCount} PK • ${fkCount} FK
                        </div>
                    </div>
                `;
            })
            .join('');

        tableList.innerHTML = tablesHtml;

        // Add click handlers for table items
        tableList.querySelectorAll('.table-item').forEach(item => {
            item.addEventListener('click', () => {
                const tableName = item.dataset.table;
                const table = this.currentData.tables.find(t => t.name === tableName);
                if (table) {
                    this.diagram.selectTable(table);
                }
            });
        });
    }

    handleTableSelection(table) {
        this.selectedTable = table;

        // Update table list selection
        document.querySelectorAll('.table-item').forEach(item => {
            item.classList.toggle('selected', item.dataset.table === table.name);
        });

        // Scroll selected table into view
        const selectedItem = document.querySelector(`.table-item[data-table="${table.name}"]`);
        if (selectedItem) {
            selectedItem.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
    }

    formatDate(dateString) {
        if (!dateString) return 'Unknown';

        try {
            const date = new Date(dateString);
            return date.toLocaleString();
        } catch {
            return dateString;
        }
    }

    showError(message) {
        // Create a simple error notification
        const errorDiv = document.createElement('div');
        errorDiv.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: #e74c3c;
            color: white;
            padding: 1rem;
            border-radius: 4px;
            z-index: 10000;
            max-width: 300px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        `;
        errorDiv.textContent = message;

        document.body.appendChild(errorDiv);

        setTimeout(() => {
            if (errorDiv.parentNode) {
                errorDiv.parentNode.removeChild(errorDiv);
            }
        }, 5000);
    }

    // Sample data loader for testing
    loadSampleData() {
        const sampleData = {
            "metadata": {
                "database_name": "sample.db",
                "generated_at": new Date().toISOString(),
                "version": "1.0.0",
                "table_count": 3,
                "relationship_count": 2
            },
            "tables": [
                {
                    "name": "users",
                    "display_name": "Users",
                    "columns": [
                        {"name": "id", "type": "INTEGER", "nullable": false, "primary_key": true, "foreign_key": false},
                        {"name": "email", "type": "VARCHAR(255)", "nullable": false, "primary_key": false, "foreign_key": false, "unique": true},
                        {"name": "name", "type": "VARCHAR(100)", "nullable": true, "primary_key": false, "foreign_key": false}
                    ],
                    "primary_keys": ["id"],
                    "indexes": [],
                    "row_count": 150
                },
                {
                    "name": "posts",
                    "display_name": "Posts",
                    "columns": [
                        {"name": "id", "type": "INTEGER", "nullable": false, "primary_key": true, "foreign_key": false},
                        {"name": "user_id", "type": "INTEGER", "nullable": false, "primary_key": false, "foreign_key": true},
                        {"name": "title", "type": "VARCHAR(255)", "nullable": false, "primary_key": false, "foreign_key": false},
                        {"name": "content", "type": "TEXT", "nullable": true, "primary_key": false, "foreign_key": false}
                    ],
                    "primary_keys": ["id"],
                    "indexes": [],
                    "row_count": 1250
                },
                {
                    "name": "comments",
                    "display_name": "Comments",
                    "columns": [
                        {"name": "id", "type": "INTEGER", "nullable": false, "primary_key": true, "foreign_key": false},
                        {"name": "post_id", "type": "INTEGER", "nullable": false, "primary_key": false, "foreign_key": true},
                        {"name": "user_id", "type": "INTEGER", "nullable": false, "primary_key": false, "foreign_key": true},
                        {"name": "content", "type": "TEXT", "nullable": false, "primary_key": false, "foreign_key": false}
                    ],
                    "primary_keys": ["id"],
                    "indexes": [],
                    "row_count": 3200
                }
            ],
            "relationships": [
                {
                    "id": "1",
                    "from_table": "posts",
                    "from_column": "user_id",
                    "to_table": "users",
                    "to_column": "id",
                    "relationship_type": "many-to-one",
                    "constraint_name": "fk_posts_user"
                },
                {
                    "id": "2",
                    "from_table": "comments",
                    "from_column": "post_id",
                    "to_table": "posts",
                    "to_column": "id",
                    "relationship_type": "many-to-one",
                    "constraint_name": "fk_comments_post"
                },
                {
                    "id": "3",
                    "from_table": "comments",
                    "from_column": "user_id",
                    "to_table": "users",
                    "to_column": "id",
                    "relationship_type": "many-to-one",
                    "constraint_name": "fk_comments_user"
                }
            ],
            "layout": {
                "algorithm": "force-directed",
                "viewport": {"center_x": 0, "center_y": 0, "zoom": 1.0}
            }
        };

        this.loadDiagramData(sampleData);
    }
}

// Initialize the application when the page loads
document.addEventListener('DOMContentLoaded', () => {
    window.app = new ERDiagramApp();

    // Add a sample data button for testing (remove in production)
    if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') {
        const sampleBtn = document.createElement('button');
        sampleBtn.textContent = 'Load Sample Data';
        sampleBtn.onclick = () => window.app.loadSampleData();
        document.querySelector('.controls').appendChild(sampleBtn);
    }
});