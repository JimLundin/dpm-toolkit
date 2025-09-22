class SimpleDiagram {
    constructor() {
        this.svg = d3.select('#diagram');
        this.width = window.innerWidth;
        this.height = window.innerHeight;
        this.svg.attr('width', this.width).attr('height', this.height);

        this.g = this.svg.append('g');
        this.createArrowMarker();
        this.processData();
        this.render();
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

    processData() {
        // Create nodes with random initial positions
        this.nodes = data.tables.map(table => ({
            id: table.name,
            ...table,
            x: Math.random() * (this.width - 200) + 100,
            y: Math.random() * (this.height - 150) + 75,
            width: Math.max(200, table.name.length * 12),
            height: 30 + (table.columns.length * 18)
        }));

        // Create links from foreign keys
        this.links = [];
        data.tables.forEach(table => {
            table.foreign_keys.forEach(fk => {
                this.links.push({
                    source: table.name,
                    target: fk.target.table_name
                });
            });
        });
    }

    render() {
        // Render relationships
        const links = this.g.selectAll('.relationship-line')
            .data(this.links)
            .enter()
            .append('line')
            .attr('class', 'relationship-line');

        // Render tables
        const tables = this.g.selectAll('.table-node')
            .data(this.nodes)
            .enter()
            .append('g')
            .attr('class', 'table-node')
            .call(d3.drag()
                .on('start', (event, d) => {
                    d3.select(event.sourceEvent.target.parentNode).raise();
                })
                .on('drag', (event, d) => {
                    d.x = event.x;
                    d.y = event.y;
                    this.updatePositions();
                }));

        // Table background
        tables.append('rect')
            .attr('class', 'table-body')
            .attr('width', d => d.width)
            .attr('height', d => d.height);

        // Table header
        tables.append('rect')
            .attr('class', 'table-header')
            .attr('width', d => d.width)
            .attr('height', 25);

        // Table name
        tables.append('text')
            .attr('class', 'table-name')
            .attr('x', d => d.width / 2)
            .attr('y', 17)
            .attr('text-anchor', 'middle')
            .text(d => d.name);

        // Columns
        tables.selectAll('.column-text')
            .data(d => d.columns.map((col, i) => ({ ...col, index: i, tableWidth: d.width })))
            .enter()
            .append('text')
            .attr('class', 'column-text')
            .attr('x', 8)
            .attr('y', (d, i) => 40 + i * 18)
            .attr('font-size', '11px')
            .text(d => {
                // Handle the new nested type structure
                let typeStr = d.type.type;
                if (d.type.length) {
                  typeStr += `(${d.type.length})`;
                }
                if (d.type.precision && d.type.scale) {
                  typeStr += `(${d.type.precision},${d.type.scale})`;
                }
                if (d.type.values) {
                  typeStr += `[${d.type.values.join('|')}]`;
                }

                // Add indicators for constraints
                let indicators = '';
                if (d.primary_key) {
                  indicators += ' 🔑';
                }
                if (d.foreign_keys && d.foreign_keys.length > 0) {
                    indicators += ' 🔗';
                }
                if (!d.nullable) {
                    indicators += ' ⚠️';
                }

                return `${d.name} (${typeStr})${indicators}`;
            });

        this.updatePositions();
    }

    updatePositions() {
        // Update table positions
        this.g.selectAll('.table-node')
            .attr('transform', d => `translate(${d.x - d.width/2}, ${d.y - d.height/2})`);

        // Update link positions
        this.g.selectAll('.relationship-line')
            .attr('x1', d => {
                const source = this.nodes.find(n => n.id === d.source);
                return source ? source.x : 0;
            })
            .attr('y1', d => {
                const source = this.nodes.find(n => n.id === d.source);
                return source ? source.y : 0;
            })
            .attr('x2', d => {
                const target = this.nodes.find(n => n.id === d.target);
                return target ? target.x : 0;
            })
            .attr('y2', d => {
                const target = this.nodes.find(n => n.id === d.target);
                return target ? target.y : 0;
            });
    }
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    new SimpleDiagram();
});

// Handle window resize
window.addEventListener('resize', () => {
    location.reload();
});