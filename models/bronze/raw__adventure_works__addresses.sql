MODEL (
  kind VIEW
);

SELECT
  *
FROM delta_scan("./lakehouse/Files/bronze/raw__adventure_works__addresses")