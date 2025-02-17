MODEL (
  kind VIEW
);
    
SELECT
  *
FROM gold._bridge__as_of
WHERE bridge__is_current_record = True