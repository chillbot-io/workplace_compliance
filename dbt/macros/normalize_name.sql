{% macro normalize_name(column_name) %}
    -- Normalize employer name: uppercase, strip non-alpha, expand abbreviations, remove suffixes
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            UPPER(TRIM({{ column_name }})),
                            '[^A-Z0-9 ]', '', 'g'          -- strip non-alphanumeric
                        ),
                        '\b(INC|INCORPORATED)\b', '', 'g'   -- remove corporate suffixes
                    ),
                    '\b(LLC|LC|L L C)\b', '', 'g'
                ),
                '\b(CORP|CORPORATION)\b', '', 'g'
            ),
            '\b(CO|COMPANY)\b', '', 'g'
        ),
        '\s+', ' ', 'g'                                    -- collapse whitespace
    )
{% endmacro %}
