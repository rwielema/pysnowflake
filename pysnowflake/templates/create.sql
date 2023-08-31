CREATE {% if data.replace %}OR REPLACE {{ type }} {% else %}{{ type }} IF NOT EXISTS {% endif %}{{ data.database }}.{{data.schema}}.{{ data.name }}
{% if data.schedule %}
SCHEDULE = 'USING {{ data.schedule }}'
{% endif %}
{%- for column in data.columns %}
    {% if loop.first %}( {% endif %}
    {{ column }}{% if not loop.last %},{% endif %}
    {% if loop.last %}){% endif %}
{%- endfor %}
{% if data.comment -%}
COMMENT '{{ data.comment }}'
{%- endif %}
{%- for rap in data.row_access_policies %}
WITH ROW ACCESS POLICY {{ rap.name }} ON {% for column in rap.columns %}{{ column }}{% if not loop.last %}, {% endif
%}{% endfor %}
{%- endfor -%}
{%- if data.tags -%}
{%- for tag in data.tags %}
WITH TAG {{ tag.name }} = '{{ tag.value }}'{% if not loop.last %},{% endif %}
{%- endfor %}
{%- endif %}
{%- if data.query %}
AS
{{ data.query }}
{%- endif %}
;