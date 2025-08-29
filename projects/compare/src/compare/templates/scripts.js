/**
 * Database Comparison Report Renderer - Streamlined
 */

class DatabaseReport {
    constructor(data) {
        this.data = this.validateData(data);
        this.rowHeight = 35;
        this.virtualThreshold = 100;
    }

    validateData(data) {
        if (!Array.isArray(data)) throw new Error('Invalid data format');
        return data.map(table => {
            // Handle NamedTuple structure: Comparison(name, TableChange(columns, rows))
            // table = [name, [columns, rows]]
            const [name, tableChange] = table;
            const [columns, rows] = tableChange;
            const [colHeaders, colChanges] = columns;
            const [rowHeaders, rowChanges] = rows;
            
            return {
                name: name,
                cols: { 
                    headers: colHeaders || [null, null],  // headers 
                    changes: colChanges || []             // changes
                },
                rows: { 
                    headers: rowHeaders || [null, null],  // headers
                    changes: rowChanges || []             // changes
                }
            };
        });
    }

    init() {
        this.renderSummary();
        this.renderTableList();
    }

    renderSummary() {
        let totalTables = this.data.length;
        let tablesWithChanges = 0, totalColChanges = 0, totalRowChanges = 0;

        this.data.forEach(table => {
            const colChanges = table.cols.changes.length;
            const rowChanges = table.rows.changes.length;
            if (colChanges > 0 || rowChanges > 0) tablesWithChanges++;
            totalColChanges += colChanges;
            totalRowChanges += rowChanges;
        });

        ['total-tables', 'tables-with-changes', 'schema-changes', 'data-changes']
            .forEach((id, i) => {
                const el = document.getElementById(id);
                if (el) el.textContent = [totalTables, tablesWithChanges, totalColChanges, totalRowChanges][i].toLocaleString();
            });
    }

    renderTableList() {
        const container = document.getElementById('tables-container');
        const tablesWithChanges = this.data.filter(t => t.cols.changes.length > 0 || t.rows.changes.length > 0);
        
        if (tablesWithChanges.length === 0) {
            container.innerHTML = '<div class="no-changes">No changes detected</div>';
            return;
        }

        container.innerHTML = '';
        tablesWithChanges.forEach(table => {
            const totalChanges = table.cols.changes.length + table.rows.changes.length;
            const section = document.createElement('div');
            section.className = 'table-container';
            section.innerHTML = `
                <h3 class="expandable-header" data-table="${table.name}">
                    ${table.name} (${totalChanges.toLocaleString()} changes)
                </h3>
                <div class="collapsible-content" id="content-${table.name}">
                    <div class="loading">Loading table details</div>
                </div>
            `;
            section.querySelector('.expandable-header').addEventListener('click', () => this.toggleTable(table));
            container.appendChild(section);
        });
    }

    toggleTable(table) {
        const header = document.querySelector(`[data-table="${table.name}"]`);
        const content = document.getElementById(`content-${table.name}`);
        
        header.classList.toggle('expanded');
        content.classList.toggle('show');

        if (content.classList.contains('show') && content.innerHTML.includes('Loading')) {
            console.log(`Loading table ${table.name}: col changes=${table.cols.changes.length}, row changes=${table.rows.changes.length}`);
            content.innerHTML = '';
            if (table.cols.changes.length > 0) content.appendChild(this.createChangeSetSection(table.cols, 'Column Changes', 'schema-table'));
            if (table.rows.changes.length > 0) content.appendChild(this.createChangeSetSection(table.rows, 'Row Changes', 'data-table', table.name));
        }
    }

    createChangeSetSection(changeSet, title, tableClass, tableName = null) {
        const section = document.createElement('div');
        const fields = this.getFields(changeSet.headers);
        const counters = this.countChangeTypes(changeSet.changes);
        const badges = Object.entries(counters)
            .filter(([_, count]) => count > 0)
            .map(([type, count]) => `<span class="change-badge ${type}">${type}</span> ${count.toLocaleString()}`)
            .join(' ');

        const tableId = tableName ? `data-table-${tableName}` : '';
        const tbodyId = tableName ? `data-tbody-${tableName}` : '';

        section.innerHTML = `
            <h4>${title} ${badges}</h4>
            <div class="table-content-wrapper">
                <table class="${tableClass}" ${tableId ? `id="${tableId}"` : ''}>
                    <thead><tr>${fields.map(f => `<th>${this.escape(f)}</th>`).join('')}</tr></thead>
                    <tbody ${tbodyId ? `id="${tbodyId}"` : ''}>${tableName ? '' : this.renderRows(changeSet, fields)}</tbody>
                </table>
            </div>
        `;

        if (tableName) {
            // Use setTimeout to ensure DOM elements are available
            setTimeout(() => this.initializeVirtualScrolling(tableName, changeSet, fields), 0);
        }
        return section;
    }

    initializeVirtualScrolling(tableName, changeSet, fields) {
        const tbody = document.getElementById(`data-tbody-${tableName}`);
        if (!tbody) {
            console.error(`Could not find tbody for virtual scrolling: data-tbody-${tableName}`);
            return;
        }

        if (changeSet.changes.length <= this.virtualThreshold) {
            this.renderAllRows(tableName, changeSet, fields);
            return;
        }

        const container = document.querySelector(`#data-table-${tableName}`).closest('.table-content-wrapper');
        if (!container) {
            console.error(`Could not find container for virtual scrolling: data-table-${tableName}`);
            return;
        }
        
        container.style.height = '500px';
        container.style.overflow = 'auto';

        let startIndex = 0;
        const renderVisible = () => this.renderVisibleRows(tbody, changeSet, fields, startIndex);
        renderVisible();

        container.addEventListener('scroll', this.debounce(() => {
            startIndex = Math.floor(container.scrollTop / this.rowHeight);
            renderVisible();
        }, 16));
    }

    renderVisibleRows(tbody, changeSet, fields, startIndex) {
        const bufferStart = Math.max(0, startIndex - 10); // Add buffer above
        const bufferEnd = Math.min(changeSet.changes.length, startIndex + 70); // Add buffer below
        const fragment = document.createDocumentFragment();

        if (bufferStart > 0) {
            const spacer = document.createElement('tr');
            spacer.style.height = `${bufferStart * this.rowHeight}px`;
            spacer.innerHTML = `<td colspan="${fields.length}" style="padding:0;border:none"></td>`;
            fragment.appendChild(spacer);
        }

        for (let i = bufferStart; i < bufferEnd; i++) {
            const row = document.createElement('tr');
            row.className = `row-${this.getChangeType(changeSet.changes[i])}`;
            row.innerHTML = this.renderChangeRow(changeSet.changes[i], changeSet.headers, fields);
            fragment.appendChild(row);
        }

        if (bufferEnd < changeSet.changes.length) {
            const spacer = document.createElement('tr');
            spacer.style.height = `${(changeSet.changes.length - bufferEnd) * this.rowHeight}px`;
            spacer.innerHTML = `<td colspan="${fields.length}" style="padding:0;border:none"></td>`;
            fragment.appendChild(spacer);
        }

        tbody.innerHTML = '';
        tbody.appendChild(fragment);
    }

    renderAllRows(tableName, changeSet, fields) {
        const tbody = document.getElementById(`data-tbody-${tableName}`);
        if (!tbody) {
            console.error(`Could not find tbody with id: data-tbody-${tableName}`);
            return;
        }
        tbody.innerHTML = this.renderRows(changeSet, fields);
    }

    renderRows(changeSet, fields) {
        return changeSet.changes.map(change => 
            `<tr class="row-${this.getChangeType(change)}">${this.renderChangeRow(change, changeSet.headers, fields)}</tr>`
        ).join('');
    }

    renderChangeRow(change, headers, fields) {
        const type = this.getChangeType(change);
        const [newVals, oldVals] = change;
        const [newFields, oldFields] = headers;

        return fields.map(field => {
            if (type === 'added') {
                const val = this.getValue(newVals, newFields, field);
                return `<td title="${this.escape(val)}">${this.formatValue(val)}</td>`;
            } else if (type === 'removed') {
                const val = this.getValue(oldVals, oldFields, field);
                return `<td title="${this.escape(val)}">${this.formatValue(val)}</td>`;
            } else {
                const newVal = this.getValue(newVals, newFields, field);
                const oldVal = this.getValue(oldVals, oldFields, field);
                if (oldVal !== newVal) {
                    return `<td class="modified-cell">
                        <span class="old-value" title="${this.escape(oldVal)}">${this.formatValue(oldVal)}</span>
                        <span class="change-arrow">→</span>
                        <span class="new-value" title="${this.escape(newVal)}">${this.formatValue(newVal)}</span>
                    </td>`;
                }
                return `<td title="${this.escape(newVal)}">${this.formatValue(newVal)}</td>`;
            }
        }).join('');
    }

    getFields(headers) {
        const fields = new Set();
        if (Array.isArray(headers)) {
            // headers is now [newHeaders, oldHeaders] where each can be an array or null
            headers.forEach(fieldArray => {
                if (Array.isArray(fieldArray)) {
                    fieldArray.forEach(f => fields.add(f));
                }
            });
        }
        return Array.from(fields);
    }

    getValue(values, headers, field) {
        if (!values || !headers) return null;
        const index = headers.indexOf(field);
        return index >= 0 && Array.isArray(values) ? values[index] : null;
    }

    getChangeType(change) {
        const [newVal, oldVal] = change;
        if (newVal && !oldVal) return 'added';
        if (oldVal && !newVal) return 'removed';
        return 'modified';
    }

    countChangeTypes(changes) {
        const counts = { added: 0, removed: 0, modified: 0 };
        changes.forEach(change => counts[this.getChangeType(change)]++);
        return counts;
    }

    formatValue(value) {
        if (value === null || value === undefined) return '<span class="null">NULL</span>';
        if (value === '') return '<span class="null">empty</span>';
        return this.escape(String(value));
    }

    escape(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    debounce(func, wait) {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func(...args), wait);
        };
    }
}

document.addEventListener('DOMContentLoaded', function() {
    if (typeof window.comparisonData === 'undefined') {
        document.getElementById('tables-container').innerHTML = 
            '<div class="error">No comparison data available</div>';
        return;
    }

    try {
        window.report = new DatabaseReport(window.comparisonData);
        window.report.init();
        console.log('Database comparison report loaded successfully');
    } catch (error) {
        console.error('Failed to load report:', error);
        document.getElementById('tables-container').innerHTML = 
            `<div class="error">Failed to load report: ${error.message}</div>`;
    }
});