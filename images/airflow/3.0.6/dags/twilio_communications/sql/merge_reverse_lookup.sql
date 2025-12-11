-- Merge reverse lookup results into the main table.
-- Updates existing records and inserts new ones
merge into {{ params.raw_database }}.{{ params.raw_schema }}.{{ params.raw_table }}
    as target
using {{ params.raw_database }}.{{ params.raw_schema }}.{{ params.raw_table }}{{ params.suffix }}
    as source
        on source.PHONE_NUMBER_E164 = target.PHONE_NUMBER_E164
when matched then update set
    {% for col in params.update_columns -%}
    {% if col in ['_LAST_LOOKUP', '_AIRFLOADED_AT'] -%}
    {{ col }} = convert_timezone('UTC', source.{{ col }})
    {%- else -%}
    {{ col }} = source.{{ col }}
    {%- endif -%}
    {%- if not loop.last %},
    {% endif %}
    {%- endfor %}
when not matched then insert (
    {{ params.insert_columns | join(', ') }}
) values (
    {% for col in params.insert_columns -%}
    source.{{ col }}
    {%- if not loop.last %}, {% endif %}
    {%- endfor %}
)