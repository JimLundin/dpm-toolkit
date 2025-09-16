/**
 * ER Diagram visualization using D3.js
 */

class ERDiagram {
    constructor(containerId) {
        this.container = d3.select(containerId);
        this.svg = this.container.select('svg');
        this.width = 0;
        this.height = 0;
        this.data = null;
        this.zoom = null;
        this.simulation = null;
        this.nodes = [];
        this.links = [];

        this.init();
    }

    init() {
        // Set up SVG dimensions
        this.updateDimensions();

        // Create zoom behavior
        this.zoom = d3.zoom()
            .scaleExtent([0.1, 3])
            .on('zoom', (event) => {
                this.g.attr('transform', event.transform);
            });

        this.svg.call(this.zoom);

        // Create main group for all elements
        this.g = this.svg.append('g');

        // Create arrow marker for relationships
        this.createArrowMarker();

        // Handle window resize
        window.addEventListener('resize', () => {
            this.updateDimensions();
            this.updateSimulation();
        });
    }

    updateDimensions() {
        const rect = this.container.node().getBoundingClientRect();
        this.width = rect.width;
        this.height = rect.height;
        this.svg.attr('width', this.width).attr('height', this.height);
    }

    createArrowMarker() {
        this.svg.append('defs')
            .append('marker')
            .attr('id', 'arrowhead')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 10)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', '#3498db');
    }

    loadData(data) {
        this.data = data;
        this.processData();
        this.render();
    }

    processData() {
        // Convert tables to nodes
        this.nodes = this.data.tables.map(table => ({
            id: table.name,
            ...table,
            x: table.position?.x || Math.random() * this.width,
            y: table.position?.y || Math.random() * this.height
        }));

        // Build relationships from foreign key definitions
        this.links = [];
        this.data.tables.forEach(table => {
            table.foreign_keys.forEach(fk => {
                // Create a link for each foreign key
                const link = {
                    source: table.name,
                    target: fk.target_table,
                    sourceColumns: fk.column_mappings.map(m => m.source_column),
                    targetColumns: fk.column_mappings.map(m => m.target_column),
                    // Generate a unique ID for this relationship
                    id: `${table.name}_${fk.target_table}_${fk.column_mappings.map(m => m.source_column).join('_')}`
                };
                this.links.push(link);
            });
        });
    }

    render() {
        this.clear();
        this.createSimulation();
        this.renderRelationships();
        this.renderTables();
        this.simulation.alpha(0.3).restart();
    }

    clear() {
        this.g.selectAll('*').remove();
        this.createArrowMarker();
    }

    createSimulation() {
        this.simulation = d3.forceSimulation(this.nodes)
            .force('link', d3.forceLink(this.links).id(d => d.id).distance(200))
            .force('charge', d3.forceManyBody().strength(-1000))
            .force('center', d3.forceCenter(this.width / 2, this.height / 2))
            .force('collision', d3.forceCollide().radius(100));
    }

    renderRelationships() {
        const linkGroup = this.g.append('g').attr('class', 'relationships');

        const links = linkGroup.selectAll('.relationship-line')
            .data(this.links)
            .enter()
            .append('line')
            .attr('class', 'relationship-line')
            .on('mouseover', (event, d) => this.showTooltip(event, d, 'relationship'))
            .on('mouseout', () => this.hideTooltip());

        this.simulation.on('tick', () => {
            links
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
        });
    }

    renderTables() {
        const tableGroup = this.g.append('g').attr('class', 'tables');

        const tables = tableGroup.selectAll('.table-node')
            .data(this.nodes)
            .enter()
            .append('g')
            .attr('class', 'table-node')
            .call(d3.drag()
                .on('start', (event, d) => this.dragStarted(event, d))
                .on('drag', (event, d) => this.dragged(event, d))
                .on('end', (event, d) => this.dragEnded(event, d)))
            .on('click', (event, d) => this.selectTable(d))
            .on('mouseover', (event, d) => this.showTooltip(event, d, 'table'))
            .on('mouseout', () => this.hideTooltip());

        // Calculate table dimensions
        tables.each(function(d) {
            const columnCount = d.columns.length;
            d.width = Math.max(200, d.name.length * 8 + 40);
            d.height = 30 + (columnCount * 20) + 10;
        });

        // Table background
        tables.append('rect')
            .attr('class', 'table-body')
            .attr('width', d => d.width)
            .attr('height', d => d.height)
            .attr('rx', 5);

        // Table header
        tables.append('rect')
            .attr('class', 'table-header')
            .attr('width', d => d.width)
            .attr('height', 30)
            .attr('rx', 5);

        // Fix header bottom corners
        tables.append('rect')
            .attr('class', 'table-header')
            .attr('y', 25)
            .attr('width', d => d.width)
            .attr('height', 5);

        // Table name
        tables.append('text')
            .attr('class', 'table-header-text')
            .attr('x', d => d.width / 2)
            .attr('y', 20)
            .attr('text-anchor', 'middle')
            .text(d => d.name);

        // Columns - include table context for PK/FK determination
        const columns = tables.selectAll('.column')
            .data(d => d.columns.map(col => ({
                ...col,
                tableName: d.name,
                tableWidth: d.width,
                isPrimaryKey: d.primary_key.includes(col.name),
                isForeignKey: d.foreign_keys.some(fk =>
                    fk.column_mappings.some(mapping => mapping.source_column === col.name)
                )
            })))
            .enter()
            .append('g')
            .attr('class', 'column');

        // Column background (for hover effect)
        columns.append('rect')
            .attr('x', 2)
            .attr('y', (d, i) => 32 + i * 20)
            .attr('width', d => d.tableWidth - 4)
            .attr('height', 18)
            .attr('fill', 'transparent')
            .on('mouseover', function() {
                d3.select(this).attr('fill', '#f8f9fa');
            })
            .on('mouseout', function() {
                d3.select(this).attr('fill', 'transparent');
            });

        // Column indicators (PK, FK, etc.)
        columns.append('circle')
            .attr('cx', 12)
            .attr('cy', (d, i) => 42 + i * 20)
            .attr('r', 4)
            .attr('class', d => {
                if (d.isPrimaryKey) return 'primary-key-indicator';
                if (d.isForeignKey) return 'foreign-key-indicator';
                if (d.nullable) return 'nullable-indicator';
                return '';
            });

        // Column names
        columns.append('text')
            .attr('class', 'column-text')
            .attr('x', 22)
            .attr('y', (d, i) => 45 + i * 20)
            .text(d => d.name);

        // Column types
        columns.append('text')
            .attr('class', 'column-type')
            .attr('x', d => d.tableWidth - 10)
            .attr('y', (d, i) => 45 + i * 20)
            .attr('text-anchor', 'end')
            .text(d => this.formatColumnType(d.type));

        // Update positions on simulation tick
        this.simulation.on('tick', () => {
            tables.attr('transform', d => `translate(${d.x - d.width/2}, ${d.y - d.height/2})`);
        });
    }

    formatColumnType(type) {
        // Simplify long type names
        return type.replace('VARCHAR', 'VARCHAR').replace('INTEGER', 'INT').substring(0, 15);
    }

    dragStarted(event, d) {
        if (!event.active) this.simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }

    dragEnded(event, d) {
        if (!event.active) this.simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }

    selectTable(table) {
        // Highlight selected table
        this.g.selectAll('.table-node').classed('selected', false);
        this.g.selectAll('.table-node')
            .filter(d => d.id === table.id)
            .classed('selected', true);

        // Trigger table selection event
        window.dispatchEvent(new CustomEvent('tableSelected', { detail: table }));
    }

    showTooltip(event, data, type) {
        const tooltip = d3.select('#tooltip');

        let content = '';
        if (type === 'table') {
            content = this.generateTableTooltip(data);
        } else if (type === 'relationship') {
            content = this.generateRelationshipTooltip(data);
        }

        tooltip.html(content)
            .classed('hidden', false)
            .style('left', (event.pageX + 10) + 'px')
            .style('top', (event.pageY - 10) + 'px');
    }

    generateTableTooltip(table) {
        const pkCount = table.primary_key.length;
        const fkCount = table.foreign_keys.length;

        return `
            <h4>${table.name}</h4>
            <div>Columns: ${table.columns.length}</div>
            <div>Primary Keys: ${pkCount}</div>
            <div>Foreign Keys: ${fkCount}</div>
        `;
    }

    generateRelationshipTooltip(rel) {
        const sourceColumns = rel.sourceColumns.join(', ');
        const targetColumns = rel.targetColumns.join(', ');

        return `
            <h4>Foreign Key</h4>
            <div>${rel.source}.[${sourceColumns}]</div>
            <div>→ ${rel.target}.[${targetColumns}]</div>
            <div>Type: many-to-one</div>
        `;
    }

    hideTooltip() {
        d3.select('#tooltip').classed('hidden', true);
    }

    resetView() {
        this.svg.transition().duration(750)
            .call(this.zoom.transform, d3.zoomIdentity);
    }

    fitToScreen() {
        if (!this.nodes.length) return;

        const bounds = this.getBounds();
        const padding = 50;

        const scale = Math.min(
            (this.width - padding * 2) / (bounds.width),
            (this.height - padding * 2) / (bounds.height)
        );

        const translate = [
            this.width / 2 - bounds.centerX * scale,
            this.height / 2 - bounds.centerY * scale
        ];

        this.svg.transition().duration(750)
            .call(this.zoom.transform, d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale));
    }

    getBounds() {
        const xs = this.nodes.map(d => d.x);
        const ys = this.nodes.map(d => d.y);

        const minX = Math.min(...xs);
        const maxX = Math.max(...xs);
        const minY = Math.min(...ys);
        const maxY = Math.max(...ys);

        return {
            minX, maxX, minY, maxY,
            width: maxX - minX,
            height: maxY - minY,
            centerX: (minX + maxX) / 2,
            centerY: (minY + maxY) / 2
        };
    }

    zoomIn() {
        this.svg.transition().duration(300)
            .call(this.zoom.scaleBy, 1.5);
    }

    zoomOut() {
        this.svg.transition().duration(300)
            .call(this.zoom.scaleBy, 0.67);
    }

    updateSimulation() {
        if (this.simulation) {
            this.simulation.force('center', d3.forceCenter(this.width / 2, this.height / 2));
            this.simulation.alpha(0.3).restart();
        }
    }
}