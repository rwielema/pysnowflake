{{ data.name }} {{ data.type }}
{% if data.not_null %}
NOT NULL
{% endif %}
{% if data.default %}
DEFAULT {{ data.default }}
{% endif %}
{% if data.auto_increment %}
AUTOINCREMENT
{% endif %}
{% if data.auto_increment_start %}
START {{ data.auto_increment_start }}
{% endif %}
{% if data.autoincrement_step %}
INCREMENT {{ data.autoincrement_step }}
{% endif %}
{% if data.masking_policy %}
WITH MASKING POLICY {{ data.masking_policy }}
{% endif %}
{% for tag in data.tags %}
    WITH TAG {{ tag.name }} = '{{ tag.value }}'{% if not loop.last %},{% endif %}
{% endfor %}
{% if data.comment %}
COMMENT '{{ data.comment }}'
{% endif %}

