{% macro col_first(relation, candidates) %}
  {%- set cols = adapter.get_columns_in_relation(relation) -%}
  {%- set names = [] -%}
  {%- for c in cols -%}{%- do names.append(c.name | lower) -%}{%- endfor -%}
  {%- for cand in candidates -%}
    {%- if cand | lower in names -%}
      {{ return(cand) }}
    {%- endif -%}
  {%- endfor -%}
  {{ exceptions.raise_compiler_error("None of " ~ candidates ~ " exist in " ~ relation) }}
{% endmacro %}
