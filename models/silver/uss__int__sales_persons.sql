MODEL (
  kind VIEW
);

WITH joined AS (
  SELECT
    bag__adventure_works__sales_persons._hook__sales_person,
    bag__adventure_works__sales_persons.sales_person__bonus,
    bag__adventure_works__sales_persons.sales_person__commission_pct,
    bag__adventure_works__sales_persons.sales_person__modified_date,
    bag__adventure_works__sales_persons.sales_person__rowguid,
    bag__adventure_works__sales_persons.sales_person__sales_last_year,
    bag__adventure_works__sales_persons.sales_person__sales_quota,
    bag__adventure_works__sales_persons.sales_person__sales_ytd,
    bag__adventure_works__persons.person__additional_contact_info AS sales_person__additional_contact_info,
    bag__adventure_works__persons.person__demographics AS sales_person__demographics,
    bag__adventure_works__persons.person__email_promotion AS sales_person__email_promotion,
    bag__adventure_works__persons.person__first_name AS sales_person__first_name,
    bag__adventure_works__persons.person__last_name AS sales_person__last_name,
    bag__adventure_works__persons.person__middle_name AS sales_person__middle_name,
    bag__adventure_works__persons.person__modified_date AS sales_person__modified_date,
    bag__adventure_works__persons.person__name_style AS sales_person__name_style,
    bag__adventure_works__persons.person__person_type AS sales_person__person_type,
    bag__adventure_works__persons.person__rowguid AS sales_person__rowguid,
    bag__adventure_works__persons.person__suffix AS sales_person__suffix,
    bag__adventure_works__persons.person__title AS sales_person__title,
    GREATEST(
      bag__adventure_works__sales_persons.sales_person__record_loaded_at,
      bag__adventure_works__persons.person__record_loaded_at
    ) AS sales_person__record_loaded_at,
    GREATEST(
      bag__adventure_works__sales_persons.sales_person__record_updated_at,
      bag__adventure_works__persons.person__record_updated_at
    ) AS sales_person__record_updated_at,
    GREATEST(
      bag__adventure_works__sales_persons.sales_person__record_valid_from,
      bag__adventure_works__persons.person__record_valid_from
    ) AS sales_person__record_valid_from,
    LEAST(
      bag__adventure_works__sales_persons.sales_person__record_valid_to,
      bag__adventure_works__persons.person__record_valid_to
    ) AS sales_person__record_valid_to,
    sales_person__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_person__is_current_record
  FROM silver.bag__adventure_works__sales_persons
  LEFT JOIN silver.bag__adventure_works__persons
    ON bag__adventure_works__sales_persons._hook__sales_person = bag__adventure_works__persons._hook__person
    AND bag__adventure_works__sales_persons.sales_person__record_valid_from < bag__adventure_works__persons.person__record_valid_to
    AND bag__adventure_works__sales_persons.sales_person__record_valid_to > bag__adventure_works__persons.person__record_valid_from
), point_in_time AS (
  SELECT
    CONCAT(_hook__sales_person::TEXT, '~epoch|valid_from', sales_person__record_valid_from)::BLOB AS _pit_hook__sales_person,
    *
  FROM joined
)
SELECT
  *
FROM point_in_time