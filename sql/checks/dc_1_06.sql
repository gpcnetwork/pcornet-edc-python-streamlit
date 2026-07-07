-- DC 1.06 | Table IIB | Data Model Conformance | Required
-- Required fields contain values outside of data model specifications.
-- Valid value sets are loaded from PCORNET_CDM.PCORNET_DC_REF.VALUESETS (canonical reference).
-- (table, field, date_col) coverage mirrors sql/section_ii/table_iib_values_outside_cdm.sql exactly.
-- Parameters: {{ current_schema }}, {{ start_date }}
{% set checks = [
    ('DEMOGRAPHIC',   'SEX',                None,           1),
    ('DEMOGRAPHIC',   'HISPANIC',           None,           2),
    ('DEMOGRAPHIC',   'RACE',               None,           3),
    ('DEMOGRAPHIC',   'GENDER_IDENTITY',    None,           4),
    ('DEMOGRAPHIC',   'SEXUAL_ORIENTATION', None,           5),
    ('ENCOUNTER',     'ENC_TYPE',           'ADMIT_DATE',   6),
    ('ENCOUNTER',     'DISCHARGE_STATUS',   'ADMIT_DATE',   7),
    ('ENROLLMENT',    'ENR_BASIS',          None,           8),
    ('DIAGNOSIS',     'DX_TYPE',            'ADMIT_DATE',   9),
    ('DIAGNOSIS',     'PDX',                'ADMIT_DATE',  10),
    ('DIAGNOSIS',     'DX_ORIGIN',          'ADMIT_DATE',  11),
    ('PROCEDURES',    'PX_TYPE',            'PX_DATE',     12),
    ('PROCEDURES',    'PPX',                'PX_DATE',     13),
    ('VITAL',         'VITAL_SOURCE',       'MEASURE_DATE',14),
    ('VITAL',         'SMOKING',            'MEASURE_DATE',15),
    ('PRESCRIBING',   'RX_BASIS',           'RX_ORDER_DATE',16),
    ('LAB_RESULT_CM', 'RESULT_LOC',         'RESULT_DATE', 17),
    ('CONDITION',     'CONDITION_TYPE',     'REPORT_DATE', 18),
    ('CONDITION',     'CONDITION_STATUS',   'REPORT_DATE', 19)
] %}
WITH violations AS (
{% for tbl, fld, date_col, ord in checks %}
    {% if not loop.first %}UNION ALL{% endif %}
    SELECT
        '{{ tbl }}' AS EXC_TABLE,
        '{{ fld }}' AS EXC_FIELD,
        COUNT(*)    AS EXC_COUNT,
        {{ ord }}   AS ROW_ORDER
    FROM {{ current_schema }}.{{ tbl }}
    WHERE {{ fld }} IS NOT NULL
      AND TRIM({{ fld }}) <> ''
      AND EXISTS (
          SELECT 1 FROM PCORNET_CDM.PCORNET_DC_REF.VALUESETS
          WHERE TABLE_NAME = '{{ tbl }}' AND FIELD_NAME = '{{ fld }}'
      )
      AND UPPER(TRIM({{ fld }})) NOT IN (
          SELECT UPPER(TRIM(VALUESET_ITEM))
          FROM PCORNET_CDM.PCORNET_DC_REF.VALUESETS
          WHERE TABLE_NAME = '{{ tbl }}' AND FIELD_NAME = '{{ fld }}'
      )
      {% if date_col %}AND {{ date_col }} >= TO_DATE('{{ start_date }}') AND {{ date_col }} <= TO_DATE('{{ end_date }}'){% endif %}
    HAVING COUNT(*) > 0
{% endfor %}
),
summary AS (
    SELECT
        '1.06'                                                                AS CHECK_NUM,
        'Required fields contain values outside of data model specifications' AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM violations) = 0 THEN 'Pass' ELSE 'Fail' END AS STATUS,
        'SUMMARY'                                                             AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                 AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                 AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                 AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                  AS EXC_COUNT,
        0                                                                     AS ROW_ORDER
),
details AS (
    SELECT
        '1.06'                                                                AS CHECK_NUM,
        'Required fields contain values outside of data model specifications' AS DESCRIPTION,
        'Fail'                                                                AS STATUS,
        'DETAIL'                                                              AS ROW_TYPE,
        v.EXC_TABLE                                                           AS EXC_TABLE,
        v.EXC_FIELD                                                           AS EXC_FIELD,
        'Out of spec'                                                         AS EXC_DETAIL,
        v.EXC_COUNT                                                           AS EXC_COUNT,
        v.ROW_ORDER                                                           AS ROW_ORDER
    FROM violations v
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
