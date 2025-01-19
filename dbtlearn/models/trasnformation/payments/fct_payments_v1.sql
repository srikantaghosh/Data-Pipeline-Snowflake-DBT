SELECT *
FROM {{ ref('stg_payments') }}
{% if is_incremental() %}
  WHERE prefix.date_col >= coalesce((select max(date_col) from {{ this }}), '1900-01-01'),
  CURRENT_DATE - INTERVAL '7 DAYS'   -- Fallback for initial run

{% endif %}