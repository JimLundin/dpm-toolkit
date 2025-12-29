# Type Refinement Analysis Report

**Database:** {{ database_name }}
**Generated:** {{ generated_at }}

## Summary

- **Total Recommendations:** {{ summary.total_recommendations }}
- **High Confidence (≥0.9):** {{ summary.by_confidence.high }}
- **Medium Confidence (0.7-0.9):** {{ summary.by_confidence.medium }}
- **Low Confidence (<0.7):** {{ summary.by_confidence.low }}

### By Type

{% for type_name, count in summary.by_type.items()|sort %}
- **{{ type_name }}:** {{ count }}
{% endfor %}

{% if patterns %}
## Discovered Naming Patterns

These patterns can be added to `type_registry.py` to improve future migrations:

{% for inferred_type, type_patterns in patterns_by_type.items()|sort %}
### {{ inferred_type|upper }} Patterns

{% for pattern in type_patterns[:5] %}
#### {{ pattern.pattern_type|capitalize }}: `{{ pattern.pattern }}`

- **Occurrences:** {{ pattern.occurrences }} ({{ "%.1f"|format(pattern.confidence * 100) }}% confidence)
- **Examples:** {{ pattern.examples|join(', ') }}

**Suggested addition to `type_registry.py`:**
```python
{% if pattern.pattern_type == "suffix" -%}
if col_name.lower().endswith("{{ pattern.pattern }}"):
    return {{ inferred_type|upper }}
{%- elif pattern.pattern_type == "prefix" -%}
if col_name.lower().startswith("{{ pattern.pattern }}"):
    return {{ inferred_type|upper }}
{%- elif pattern.pattern_type == "exact" -%}
if col_name.lower() == "{{ pattern.pattern }}":
    return {{ inferred_type|upper }}
{%- endif %}
```

{% endfor %}
{% endfor %}
{% else %}
## Discovered Naming Patterns

No patterns discovered.
{% endif %}

{% if recommendations %}
## Detailed Recommendations

{% for inferred_type, type_recs in recommendations_by_type.items()|sort %}
### {{ inferred_type|upper }} Candidates

{% for rec in type_recs[:max_recommendations] %}
#### {{ rec.table_name }}.{{ rec.column_name }}

- **Current Type:** {{ rec.current_type }}
- **Confidence:** {{ "%.1f"|format(rec.confidence * 100) }}%
{% if rec.inferred_type == 'enum' and rec.enum_values -%}
- **Values:** {% if rec.enum_values|length <= max_enum_values %}{{ rec.enum_values|sort|join(', ') }}{% else %}{{ rec.enum_values|sort|list|slice(max_enum_values)|first|join(', ') }} ... ({{ rec.enum_values|length }} total){% endif %}
{% endif -%}
{% if rec.detected_format -%}
- **Format:** {{ rec.detected_format }}
{% endif -%}
{% if rec.evidence -%}
- **Evidence:**
{% for key, value in rec.evidence.items() %}  - {{ key }}: {{ value }}
{% endfor -%}
{% endif %}
{% endfor %}

{% if type_recs|length > max_recommendations %}
*Showing top {{ max_recommendations }} of {{ type_recs|length }} {{ inferred_type }} candidates*
{% endif %}

{% endfor %}
{% else %}
## Recommendations

No recommendations found.
{% endif %}
