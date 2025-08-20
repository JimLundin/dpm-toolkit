/**
 * Database Comparison Report Renderer
 * Refactored for better structure, maintainability, and performance
 */

class DataProcessor {
    constructor(data) {
        this.data = this.validateData(data);
    }

    validateData(data) {
        if (!Array.isArray(data)) {
            throw new Error('Invalid data format: expected array');
        }
        return data.map((table) => this.validateTable(table));
    }

    validateTable(table) {
        if (!table.name || typeof table.name !== 'string') {
            throw new Error('Invalid table: missing name');
        }
        if (!table.cols || !table.rows) {
            throw new Error(`Invalid table ${table.name}: missing cols or rows`);
        }
        return {
            name: table.name,
            cols: this.validateChangeSet(table.cols),
            rows: this.validateChangeSet(table.rows)
        };
    }

    validateChangeSet(changeSet) {
        return {
            headers: changeSet.headers || [null, null],
            changes: Array.isArray(changeSet.changes) 
                ? changeSet.changes 
                : Array.from(changeSet.changes || [])
        };
    }

    getChangesCount(changes) {
        return changes ? changes.length : 0;
    }

    getChangeType(change) {
        if (!Array.isArray(change) || change.length !== 2) {
            console.warn('Invalid change format:', change);
            return 'unknown';
        }
        
        const [newVal, oldVal] = change;
        if (newVal && !oldVal) return 'added';
        if (oldVal && !newVal) return 'removed';
        return 'modified';
    }

    getFieldsFromHeaders(headers) {
        const fields = new Set();
        
        // Headers is an array [newFields, oldFields]
        if (Array.isArray(headers)) {
            const [newFields, oldFields] = headers;
            
            if (newFields && Array.isArray(newFields)) {
                newFields.forEach(field => fields.add(field));
            }
            if (oldFields && Array.isArray(oldFields)) {
                oldFields.forEach(field => fields.add(field));
            }
        }
        
        return Array.from(fields);
    }

    countChangeTypes(changes) {
        return changes.reduce((counts, change) => {
            const type = this.getChangeType(change);
            counts[type] = (counts[type] || 0) + 1;
            return counts;
        }, { added: 0, removed: 0, modified: 0 });
    }
}

class SummaryRenderer {
    constructor(dataProcessor) {
        this.data = dataProcessor;
    }

    render() {
        const stats = this.calculateStats();
        this.updateSummaryCard('total-tables', stats.totalTables);
        this.updateSummaryCard('tables-with-changes', stats.tablesWithChanges);
        this.updateSummaryCard('schema-changes', stats.totalColChanges);
        this.updateSummaryCard('data-changes', stats.totalRowChanges);
    }

    calculateStats() {
        const tables = this.data.data;
        const totalTables = tables.length;
        
        let tablesWithChanges = 0;
        let totalColChanges = 0;
        let totalRowChanges = 0;

        tables.forEach(table => {
            const colChanges = this.data.getChangesCount(table.cols.changes);
            const rowChanges = this.data.getChangesCount(table.rows.changes);
            
            if (colChanges > 0 || rowChanges > 0) {
                tablesWithChanges++;
            }
            
            totalColChanges += colChanges;
            totalRowChanges += rowChanges;
        });

        return { totalTables, tablesWithChanges, totalColChanges, totalRowChanges };
    }

    updateSummaryCard(elementId, value) {
        const element = document.getElementById(elementId);
        if (element) {
            element.textContent = value.toLocaleString();
        }
    }
}

class TableRenderer {
    constructor(dataProcessor) {
        this.data = dataProcessor;
        this.virtualScrollManager = new VirtualScrollManager();
    }

    renderTableList(container) {
        const tablesWithChanges = this.data.data.filter(table => 
            this.data.getChangesCount(table.cols.changes) > 0 || 
            this.data.getChangesCount(table.rows.changes) > 0
        );
        
        container.innerHTML = '';
        
        if (tablesWithChanges.length === 0) {
            container.innerHTML = '<div class="no-changes">No changes detected between databases</div>';
            return;
        }

        const fragment = document.createDocumentFragment();
        
        tablesWithChanges.forEach(table => {
            const tableElement = this.createTableSection(table);
            fragment.appendChild(tableElement);
        });

        container.appendChild(fragment);
    }

    createTableSection(table) {
        const section = document.createElement('div');
        section.className = 'table-container';
        
        const colCount = this.data.getChangesCount(table.cols.changes);
        const rowCount = this.data.getChangesCount(table.rows.changes);
        const totalChanges = colCount + rowCount;

        const header = document.createElement('h3');
        header.className = 'expandable-header';
        header.textContent = `${table.name} (${totalChanges.toLocaleString()} changes)`;
        header.setAttribute('data-table', table.name);
        header.addEventListener('click', () => this.toggleTable(table.name));

        const content = document.createElement('div');
        content.className = 'collapsible-content';
        content.id = `content-${table.name}`;
        content.innerHTML = '<div class="loading">Loading table details</div>';

        section.appendChild(header);
        section.appendChild(content);
        
        return section;
    }

    toggleTable(tableName) {
        try {
            const header = document.querySelector(`[data-table="${tableName}"]`);
            const content = document.getElementById(`content-${tableName}`);
            
            if (!header || !content) {
                console.error(`Could not find table elements for: ${tableName}`);
                return;
            }
            
            header.classList.toggle('expanded');
            content.classList.toggle('show');

            if (content.classList.contains('show') && 
                content.innerHTML.includes('Loading table details')) {
                this.loadTableContent(tableName);
            }
        } catch (error) {
            console.error(`Error toggling table ${tableName}:`, error);
        }
    }

    loadTableContent(tableName) {
        try {
            const table = this.data.data.find(t => t.name === tableName);
            const content = document.getElementById(`content-${tableName}`);
            
            if (!table) {
                content.innerHTML = '<div class="error">Table data not found</div>';
                return;
            }
            
            const fragment = document.createDocumentFragment();

            if (this.data.getChangesCount(table.cols.changes) > 0) {
                const colSection = this.renderChangeSet(
                    table.cols, 
                    'Column Changes', 
                    'schema-table'
                );
                fragment.appendChild(colSection);
            }

            if (this.data.getChangesCount(table.rows.changes) > 0) {
                const rowSection = this.renderChangeSet(
                    table.rows, 
                    'Row Changes', 
                    'data-table',
                    `data-table-${tableName}`,
                    `data-tbody-${tableName}`
                );
                fragment.appendChild(rowSection);
            }

            content.innerHTML = '';
            content.appendChild(fragment);

            if (this.data.getChangesCount(table.rows.changes) > 0) {
                this.initializeDataTable(tableName, table.rows);
            }

        } catch (error) {
            console.error(`Error loading table content for ${tableName}:`, error);
            const content = document.getElementById(`content-${tableName}`);
            if (content) {
                content.innerHTML = `<div class="error">Error loading table: ${error.message}</div>`;
            }
        }
    }

    renderChangeSet(changeSet, title, tableClass, tableId = null, tbodyId = null) {
        const section = document.createElement('div');
        const counters = this.data.countChangeTypes(changeSet.changes);
        const fields = this.data.getFieldsFromHeaders(changeSet.headers);
        
        const badges = Object.entries(counters)
            .filter(([_, count]) => count > 0)
            .map(([type, count]) => 
                `<span class="change-badge ${type}">${type}</span> ${count.toLocaleString()}`
            )
            .join(' ');

        section.innerHTML = `
            <h4>${title} ${badges}</h4>
            <div class="table-content-wrapper">
                <table class="${tableClass}"${tableId ? ` id="${tableId}"` : ''}>
                    <thead>
                        <tr>${fields.map(field => `<th>${this.escapeHtml(field)}</th>`).join('')}</tr>
                    </thead>
                    <tbody${tbodyId ? ` id="${tbodyId}"` : ''}>
                        ${tbodyId ? '' : this.renderAllRows(changeSet, fields)}
                    </tbody>
                </table>
            </div>
        `;

        return section;
    }

    renderAllRows(changeSet, fields) {
        return changeSet.changes.map(change => {
            const changeType = this.data.getChangeType(change);
            const rowClass = `row-${changeType}`;
            return `<tr class="${rowClass}">${this.renderChangeRow(change, changeSet.headers, fields)}</tr>`;
        }).join('');
    }

    renderChangeRow(change, headers, fields) {
        const changeType = this.data.getChangeType(change);
        const [newValues, oldValues] = change;
        const [newFields, oldFields] = headers;
        
        return fields.map(field => {
            if (changeType === 'added') {
                const val = this.getValueFromRow(newValues, newFields, field);
                return `<td title="${this.escapeHtml(val)}">${this.formatValue(val)}</td>`;
            } else if (changeType === 'removed') {
                const val = this.getValueFromRow(oldValues, oldFields, field);
                return `<td title="${this.escapeHtml(val)}">${this.formatValue(val)}</td>`;
            } else {
                const newVal = this.getValueFromRow(newValues, newFields, field);
                const oldVal = this.getValueFromRow(oldValues, oldFields, field);
                
                if (oldVal !== newVal) {
                    return `<td class="modified-cell">
                        <span class="old-value" title="${this.escapeHtml(oldVal)}">${this.formatValue(oldVal)}</span>
                        <span class="change-arrow">→</span>
                        <span class="new-value" title="${this.escapeHtml(newVal)}">${this.formatValue(newVal)}</span>
                    </td>`;
                }
                return `<td title="${this.escapeHtml(newVal)}">${this.formatValue(newVal)}</td>`;
            }
        }).join('');
    }

    getValueFromRow(values, headers, field) {
        if (!values || !headers || !Array.isArray(headers)) {
            return null;
        }
        
        const index = headers.indexOf(field);
        return index >= 0 && Array.isArray(values) ? values[index] : null;
    }

    formatValue(value) {
        if (value === null || value === undefined) {
            return '<span class="null">NULL</span>';
        }
        if (value === '') {
            return '<span class="null">empty</span>';
        }
        return this.escapeHtml(String(value));
    }

    escapeHtml(text) {
        if (text === null || text === undefined) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    initializeDataTable(tableName, changeSet) {
        const changes = changeSet.changes;
        const tbody = document.getElementById(`data-tbody-${tableName}`);
        
        if (!tbody || !changes.length) return;

        if (changes.length > 100) {
            this.virtualScrollManager.initialize(tableName, changeSet, this);
        } else {
            this.renderAllDataRows(tableName, changeSet);
        }
    }

    renderAllDataRows(tableName, changeSet) {
        const tbody = document.getElementById(`data-tbody-${tableName}`);
        const fields = this.data.getFieldsFromHeaders(changeSet.headers);
        
        if (!tbody) return;

        const fragment = document.createDocumentFragment();
        
        changeSet.changes.forEach(change => {
            const row = document.createElement('tr');
            const changeType = this.data.getChangeType(change);
            row.className = `row-${changeType}`;
            row.innerHTML = this.renderChangeRow(change, changeSet.headers, fields);
            fragment.appendChild(row);
        });

        tbody.innerHTML = '';
        tbody.appendChild(fragment);
    }
}

class VirtualScrollManager {
    constructor() {
        this.rowHeight = 35;
        this.visibleRows = 50;
        this.bufferSize = 10;
    }

    initialize(tableName, changeSet, renderer) {
        const changes = changeSet.changes;
        
        if (changes.length <= 100) {
            // For small datasets, render all rows normally
            renderer.renderAllDataRows(tableName, changeSet);
            return;
        }

        // For large datasets, use virtual scrolling
        const container = document.querySelector(`#data-table-${tableName}`).closest('.table-content-wrapper');
        const tbody = document.getElementById(`data-tbody-${tableName}`);
        
        if (!container || !tbody) return;

        // Set up scrollable container
        container.style.height = '500px';
        container.style.overflow = 'auto';

        let startIndex = 0;
        this.renderVisibleRows(tableName, changeSet, renderer, startIndex);

        const scrollHandler = this.debounce(() => {
            const scrollTop = container.scrollTop;
            startIndex = Math.floor(scrollTop / this.rowHeight);
            this.renderVisibleRows(tableName, changeSet, renderer, startIndex);
        }, 16);

        container.addEventListener('scroll', scrollHandler);
    }

    renderVisibleRows(tableName, changeSet, renderer, startIndex) {
        const tbody = document.getElementById(`data-tbody-${tableName}`);
        const changes = changeSet.changes;
        const fields = renderer.data.getFieldsFromHeaders(changeSet.headers);
        
        const endIndex = Math.min(startIndex + this.visibleRows + this.bufferSize, changes.length);
        const fragment = document.createDocumentFragment();

        // Create spacer for rows above (single row with calculated height)
        if (startIndex > 0) {
            const topSpacer = document.createElement('tr');
            topSpacer.style.height = `${startIndex * this.rowHeight}px`;
            topSpacer.innerHTML = `<td colspan="${fields.length}" style="padding: 0; border: none;"></td>`;
            fragment.appendChild(topSpacer);
        }

        // Render visible rows (normal table rows - no absolute positioning)
        for (let i = startIndex; i < endIndex; i++) {
            const change = changes[i];
            const row = document.createElement('tr');
            const changeType = renderer.data.getChangeType(change);
            row.className = `row-${changeType}`;
            row.innerHTML = renderer.renderChangeRow(change, changeSet.headers, fields);
            fragment.appendChild(row);
        }

        // Create spacer for rows below
        if (endIndex < changes.length) {
            const bottomSpacer = document.createElement('tr');
            bottomSpacer.style.height = `${(changes.length - endIndex) * this.rowHeight}px`;
            bottomSpacer.innerHTML = `<td colspan="${fields.length}" style="padding: 0; border: none;"></td>`;
            fragment.appendChild(bottomSpacer);
        }

        tbody.innerHTML = '';
        tbody.appendChild(fragment);
    }

    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
}

class DatabaseReportApp {
    constructor(comparisonData) {
        try {
            this.dataProcessor = new DataProcessor(comparisonData);
            this.summaryRenderer = new SummaryRenderer(this.dataProcessor);
            this.tableRenderer = new TableRenderer(this.dataProcessor);
            this.initialized = true;
        } catch (error) {
            console.error('Failed to initialize app:', error);
            this.initialized = false;
            this.handleError(error);
        }
    }

    init() {
        if (!this.initialized) {
            console.error('App not properly initialized');
            return;
        }

        try {
            this.summaryRenderer.render();
            
            const container = document.getElementById('tables-container');
            if (container) {
                this.tableRenderer.renderTableList(container);
            }
            
            console.log('Database comparison report loaded successfully');
        } catch (error) {
            console.error('Failed to render report:', error);
            this.handleError(error);
        }
    }

    handleError(error) {
        const container = document.getElementById('tables-container');
        if (container) {
            container.innerHTML = `<div class="error">Failed to load report: ${error.message}</div>`;
        }
    }
}

document.addEventListener('DOMContentLoaded', function() {
    if (typeof window.comparisonData === 'undefined') {
        console.error('No comparison data provided');
        document.getElementById('tables-container').innerHTML = 
            '<div class="error">No comparison data available</div>';
        return;
    }

    console.log('Initializing report with data:', window.comparisonData);
    
    try {
        window.reportApp = new DatabaseReportApp(window.comparisonData);
        window.reportApp.init();
    } catch (error) {
        console.error('Failed to create or initialize app:', error);
        document.getElementById('tables-container').innerHTML = 
            `<div class="error">Failed to load report: ${error.message}</div>`;
    }
});