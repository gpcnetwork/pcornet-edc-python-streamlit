-- DC 1.06: Required fields contain values outside of data model specifications
-- Parameters: {{ current_schema }}
WITH valid_enc AS (SELECT v FROM (VALUES ('AV'),('TH'),('OA'),('ED'),('IP'),('EI'),('IS'),('OS'),('NI'),('UN'),('OT')) t(v)),
out_of_spec AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE NOT IN (SELECT v FROM valid_enc) AND ENC_TYPE IS NOT NULL
)
SELECT
    '1.06'                                             AS CHECK_NUM,
    'Required fields contain values outside of data model specifications' AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END        AS STATUS
FROM out_of_spec
