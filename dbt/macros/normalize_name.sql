{% macro normalize_name(column_name) %}
    -- Normalize employer name for entity resolution matching.
    -- DuckDB REGEXP_REPLACE needs 'g' flag for global replacement.
    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        UPPER(TRIM({{ column_name }})),
                                        '[^A-Z0-9 ]', '', 'g'
                                    ),
                                    '^[0-9]+ ', '', 'g'
                                ),
                                '( |^)(INC|INCORPORATED)( |$)', ' ', 'g'
                            ),
                            '( |^)(LLC|LC|LLP|LP|LTD|LIMITED)( |$)', ' ', 'g'
                        ),
                        '( |^)(CORP|CORPORATION)( |$)', ' ', 'g'
                    ),
                    '( |^)(CO|COMPANY|COMPANIES)( |$)', ' ', 'g'
                ),
                '( |^)(DBA|DOING BUSINESS AS)( |$)', ' ', 'g'
            ),
            '[0-9]+$', '', 'g'
        ),
        ' +', ' ', 'g'
    ))
{% endmacro %}
