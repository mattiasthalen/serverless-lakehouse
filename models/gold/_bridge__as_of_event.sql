MODEL (
  kind VIEW
);

SELECT
  *
FROM silver.uss_bridge
WHERE
  1 = 1
  AND STR_SPLIT(_hook__calendar__date::TEXT, '|')[-1]::DATE >= bridge__record_valid_from
  AND STR_SPLIT(_hook__calendar__date::TEXT, '|')[-1]::DATE <= bridge__record_valid_to