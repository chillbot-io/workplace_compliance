{% macro normalize_name(column_name) %}
    -- Normalize employer name for entity resolution matching.
    -- DuckDB uses RE2 regex (no \b word boundaries).
    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
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
                            '( |^)(LLC|LC|LLP|LP|LTD|LIMITED)( |$)', ' '
                        ),
                        '( |^)(CORP|CORPORATION)( |$)', ' '
                    ),
                    '( |^)(CO|COMPANY|COMPANIES)( |$)', ' '
                ),
                '( |^)(DBA|DOING BUSINESS AS)( |$)', ' '
            ),
            '[0-9]+$', ''
        ),
        ' +', ' '
    ))
{% endmacro %}
