{% macro normalize_name(column_name) %}
    -- Normalize employer name: uppercase, strip non-alpha, expand abbreviations, remove suffixes
    -- DuckDB uses RE2 which does NOT support \b word boundaries.
    -- After stripping non-alphanumeric chars, suffixes are space-delimited so we match on spaces.
    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            UPPER(TRIM({{ column_name }})),
                            '[^A-Z0-9 ]', ''
                        ),
                        '( |^)(INC|INCORPORATED)( |$)', ' '
                    ),
                    '( |^)(LLC|LC)( |$)', ' '
                ),
                '( |^)(CORP|CORPORATION)( |$)', ' '
            ),
            '( |^)(CO|COMPANY)( |$)', ' '
        ),
        ' +', ' '
    ))
{% endmacro %}
