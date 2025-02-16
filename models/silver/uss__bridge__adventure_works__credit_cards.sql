MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    'credit_cards' AS stage,
    bag__adventure_works__credit_cards._pit_hook__credit_card,
    bag__adventure_works__credit_cards._hook__credit_card,
    bag__adventure_works__credit_cards.credit_card__record_loaded_at AS bridge__record_loaded_at,
    bag__adventure_works__credit_cards.credit_card__record_updated_at AS bridge__record_updated_at,
    bag__adventure_works__credit_cards.credit_card__record_valid_from AS bridge__record_valid_from,
    bag__adventure_works__credit_cards.credit_card__record_valid_to AS bridge__record_valid_to
  FROM silver.bag__adventure_works__credit_cards
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge