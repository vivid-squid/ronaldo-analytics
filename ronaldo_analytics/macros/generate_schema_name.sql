{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
      If a model has +schema: STAGING/CLEAN/ANALYTICS, use that schema exactly.
      Otherwise fall back to the target schema from profiles.yml.
    #}
    {% if custom_schema_name is none %}
        {{ target.schema }}
    {% else %}
        {{ custom_schema_name | trim }}
    {% endif %}
{%- endmacro %}
