# Type Refinement Analysis Report

**Database:** {{ database_name }}
**Generated:** {{ generated_at }}

## Summary

- **Total Recommendations:** {{ summary.total_recommendations }}

### By Type

{% for type_name, count in summary.by_type.items()|sort %}
- **{{ type_name }}:** {{ count }}
{% endfor %}

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
