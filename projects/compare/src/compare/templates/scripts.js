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
        const tables = this.data;
        const totalTables = tables.length;
        const tablesWithChanges = tables.filter(
            t => t.cols.length > 0 || t.rows.length > 0
        );
        const totalColChanges = tables.reduce((sum, t) => sum + t.cols.length, 0);
        const totalRowChanges = tables.reduce((sum, t) => sum + t.rows.length, 0);

        document.getElementById('total-tables').textContent = totalTables;
        document.getElementById('tables-with-changes').textContent = tablesWithChanges.length;
        document.getElementById('schema-changes').textContent = totalColChanges;
        document.getElementById('data-changes').textContent = totalRowChanges;
    }

    renderTableList() {
        const container = document.getElementById('tables-container');
        const tables = this.data.filter(
            t => t.cols.length > 0 || t.rows.length > 0
        );
        
        // Clear loading message
        container.innerHTML = '';
        
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
        
        const colCount = table.cols.length;
        const rowCount = table.rows.length;
        const totalChanges = colCount + rowCount;

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
        try {
            const header = document.querySelector(`h3[onclick*="'${tableName}'"]`);
            const content = document.getElementById(`content-${tableName}`);
            
            if (!header || !content) {
                console.error(`Could not find table elements for: ${tableName}`);
                return;
            }
            
            header.classList.toggle('expanded');
            content.classList.toggle('show');

            if (content.classList.contains('show') && 
                content.innerHTML.includes('Loading...')) {
                this.loadTableContent(tableName);
            }
        } catch (error) {
            console.error(`Error toggling table ${tableName}:`, error);
        }
    }

    loadTableContent(tableName) {
        try {
            const table = this.data.find(t => t.name === tableName);
            const content = document.getElementById(`content-${tableName}`);
            
            if (!table) {
                console.error(`Table not found in data: ${tableName}`);
                content.innerHTML = '<div class="error">Table data not found</div>';
                return;
            }
            
            if (!content) {
                console.error(`Content element not found: content-${tableName}`);
                return;
            }
            
            let html = '';

            // Column changes
            if (table.cols && table.cols.length > 0) {
                html += this.renderColsSection(table);
            }

            // Row changes with virtual scrolling
            if (table.rows && table.rows.length > 0) {
                html += this.renderRowsSection(table);
            }

            content.innerHTML = html;

            // Initialize rendering for data tables
            if (table.rows && table.rows.length > 0) {
                if (table.rows.length > this.maxVisibleRows) {
                    this.initVirtualScrolling(tableName, table.rows);
                } else {
                    // For smaller datasets, render all rows directly
                    this.renderAllDataRows(tableName, table.rows);
                }
            }
        } catch (error) {
            console.error(`Error loading table content for ${tableName}:`, error);
            const content = document.getElementById(`content-${tableName}`);
            if (content) {
                content.innerHTML = `<div class="error">Error loading table: ${error.message}</div>`;
            }
        }
    }

    renderColsSection(table) {
        const changes = table.cols;
        const counters = this.countChangeTypes(changes);
        
        let html = `
            <h4>Column Changes
                ${counters.added > 0 ? 
                  `<span class="cb a">Added</span> ${counters.added}` : ''}
                ${counters.removed > 0 ? 
                  `<span class="cb r">Removed</span> ${counters.removed}` : ''}
                ${counters.modified > 0 ? 
                  `<span class="cb m">Modified</span> ${counters.modified}` : ''}
            </h4>
            <div class="tct">
                <table class="st">
                    <thead>
                        <tr>
                            <th>Column</th>
                            <th>Type</th>
                            <th>Nullable</th>
                            <th>Default</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        changes.forEach(change => {
            html += this.renderColRow(change);
        });

        html += `
                    </tbody>
                </table>
            </div>
        `;

        return html;
    }

    renderColRow(change) {
        const changeType = this.getChangeType(change);
        const changeClass = changeType === 'added' ? 'ca' : 
                           changeType === 'removed' ? 'cr' : 'cm';
        
        let html = `<tr class="${changeClass}">`;
        
        // NamedTuple structure: [new, old]
        const [newCol, oldCol] = change;
        const columnName = newCol?.name || oldCol?.name;
        html += `<td title="${columnName}">${columnName}</td>`;

        if (changeType === 'added') {
            html += `<td title="${newCol.type}">${newCol.type}</td>`;
            html += `<td>${newCol.nullable}</td>`;
            html += `<td>${this.formatValue(newCol.default)}</td>`;
        } else if (changeType === 'removed') {
            html += `<td title="${oldCol.type}">${oldCol.type}</td>`;
            html += `<td>${oldCol.nullable}</td>`;
            html += `<td>${this.formatValue(oldCol.default)}</td>`;
        } else {
            // Modified - show old→new changes
            html += this.renderColFieldChange(oldCol.type, newCol.type);
            html += this.renderColFieldChange(oldCol.nullable, newCol.nullable);
            html += this.renderColFieldChange(oldCol.default, newCol.default);
        }

        html += `</tr>`;
        return html;
    }

    renderColFieldChange(oldValue, newValue) {
        // Format values for comparison
        const formattedOld = this.formatValue(oldValue);
        const formattedNew = this.formatValue(newValue);
        
        if (formattedOld !== formattedNew) {
            return `<td class="mc">
                <span class="ov" title="${oldValue || 'NULL'}">
                    ${formattedOld}
                </span>
                <span class="ar">→</span>
                <span class="nv" title="${newValue || 'NULL'}">
                    ${formattedNew}
                </span>
            </td>`;
        } else {
            return `<td title="${newValue || 'NULL'}">
                ${formattedNew}
            </td>`;
        }
    }

    renderRowsSection(table) {
        const changes = table.rows;
        const counters = this.countChangeTypes(changes);
        const allColumns = this.getAllColumns(changes);
        
        let html = `
            <h4>Row Changes
                ${counters.added > 0 ? 
                  `<span class="cb a">Added</span> ${counters.added}` : ''}
                ${counters.removed > 0 ? 
                  `<span class="cb r">Removed</span> ${counters.removed}` : ''}
                ${counters.modified > 0 ? 
                  `<span class="cb m">Modified</span> ${counters.modified}` : ''}
            </h4>
        `;

        // Remove non-functional search bar

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

    renderAllDataRows(tableName, dataChanges) {
        try {
            const tbody = document.getElementById(`data-tbody-${tableName}`);
            if (!tbody) {
                console.error(`Data tbody not found: data-tbody-${tableName}`);
                return;
            }
            
            const allColumns = this.getAllColumns(dataChanges);
            
            // Clear any existing content
            tbody.innerHTML = '';
            
            // Render all rows directly (no virtual scrolling)
            dataChanges.forEach(change => {
                const row = document.createElement('tr');
                const changeType = this.getChangeType(change);
                row.className = changeType === 'added' ? 'ca' : 
                               changeType === 'removed' ? 'cr' : 'cm';
                row.innerHTML = this.renderDataRowContent(change, allColumns);
                tbody.appendChild(row);
            });
        } catch (error) {
            console.error(`Error rendering data rows for ${tableName}:`, error);
        }
    }

    initVirtualScrolling(tableName, dataChanges) {
        try {
            const tbody = document.getElementById(`data-tbody-${tableName}`);
            if (!tbody) {
                console.error(`Virtual scrolling tbody not found: data-tbody-${tableName}`);
                return;
            }
            
            const container = tbody.closest('.tct');
            if (!container) {
                console.error(`Virtual scrolling container not found for: ${tableName}`);
                return;
            }
            
            const allColumns = this.getAllColumns(dataChanges);
            
            // Set initial height
            tbody.style.height = `${dataChanges.length * this.rowHeight}px`;
            
            // Render initial visible rows
            this.renderVisibleRows(
                tableName, dataChanges, allColumns, 0, this.maxVisibleRows
            );
            
            // Add scroll listener
            container.addEventListener('scroll', () => {
                this.handleScroll(tableName, dataChanges, allColumns, container);
            });
        } catch (error) {
            console.error(`Error initializing virtual scrolling for ${tableName}:`, error);
            // Fallback to rendering all rows
            this.renderAllDataRows(tableName, dataChanges);
        }
    }

    handleScroll(tableName, dataChanges, allColumns, container) {
        const scrollTop = container.scrollTop;
        const startIndex = Math.floor(scrollTop / this.rowHeight);
        const endIndex = Math.min(startIndex + this.maxVisibleRows, dataChanges.length);
        
        this.renderVisibleRows(
            tableName, dataChanges, allColumns, startIndex, endIndex
        );
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
            row.className = 'virtual-row ' + 
                (this.getChangeType(change) === 'added' ? 'ca' : 
                 this.getChangeType(change) === 'removed' ? 'cr' : 'cm');
            row.innerHTML = this.renderDataRowContent(change, allColumns);
            tbody.appendChild(row);
        }
        
        // Add spacer for content below
        if (endIndex < dataChanges.length) {
            const spacer = document.createElement('tr');
            spacer.style.height = 
                `${(dataChanges.length - endIndex) * this.rowHeight}px`;
            spacer.innerHTML = `<td colspan="${allColumns.length}"></td>`;
            tbody.appendChild(spacer);
        }
    }

    renderDataRowContent(change, allColumns) {
        const changeType = this.getChangeType(change);
        let html = '';

        allColumns.forEach(col => {
            // NamedTuple structure: [new, old]
            const [newRow, oldRow] = change;
            
            if (changeType === 'added') {
                const val = newRow?.[col];
                html += `<td title="${val || 'NULL'}">${this.formatValue(val)}</td>`;
            } else if (changeType === 'removed') {
                const val = oldRow?.[col];
                html += `<td title="${val || 'NULL'}">${this.formatValue(val)}</td>`;
            } else {
                const oldVal = oldRow?.[col];
                const newVal = newRow?.[col];
                if (oldVal !== newVal) {
                    html += `<td class="mc">
                        <span class="ov" title="${oldVal || 'NULL'}">
                            ${this.formatValue(oldVal)}
                        </span>
                        <span class="ar">→</span>
                        <span class="nv" title="${newVal || 'NULL'}">
                            ${this.formatValue(newVal)}
                        </span>
                    </td>`;
                } else {
                    html += `<td title="${newVal || 'NULL'}">
                        ${this.formatValue(newVal)}
                    </td>`;
                }
            }
        });

        return html;
    }


    getAllColumns(changes) {
        const columns = new Set();
        changes.forEach(change => {
            // NamedTuple structure: [new, old]
            const [newVal, oldVal] = change;
            if (newVal && typeof newVal === 'object') {
                Object.keys(newVal).forEach(col => columns.add(col));
            }
            if (oldVal && typeof oldVal === 'object') {
                Object.keys(oldVal).forEach(col => columns.add(col));
            }
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
        // NamedTuple structure: [new, old]
        const [newVal, oldVal] = change;
        if (newVal && !oldVal) return 'added';
        if (oldVal && !newVal) return 'removed';
        return 'modified';
    }

    formatValue(value) {
        if (value === null || value === undefined) return '<span class="null">NULL</span>';
        if (value === '') return '';
        return String(value);
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    window.renderer = new DatabaseReportRenderer(window.comparisonData);
    window.renderer.init();
});