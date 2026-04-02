{% macro normalize_name(column_name) %}
    -- Normalize employer name: uppercase, strip non-alpha, expand abbreviations, remove suffixes
    -- DuckDB regexp_replace replaces all occurrences by default (no 'g' flag needed)
    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            UPPER(TRIM({{ column_name }})),
                            '[^A-Z0-9 ]', ''               -- strip non-alphanumeric
                        ),
                        '\b(INC|INCORPORATED)\b', ''        -- remove corporate suffixes
                    ),
                    '\b(LLC|LC|L L C)\b', ''
                ),
                '\b(CORP|CORPORATION)\b', ''
            ),
            '\b(CO|COMPANY)\b', ''
        ),
        '\s+', ' '                                          -- collapse whitespace
    ))
{% endmacro %}
