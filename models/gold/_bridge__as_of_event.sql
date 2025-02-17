MODEL (
  kind VIEW
);

SELECT
  *
FROM silver.uss_bridge
WHERE
  1 = 1
  AND string_split(_hook__calendar__date::TEXT, '|')[-1]::DATE >= bridge__record_valid_from
  AND string_split(_hook__calendar__date::TEXT, '|')[-1]::DATE <= bridge__record_valid_to