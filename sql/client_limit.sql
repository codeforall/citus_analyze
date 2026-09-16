CREATE OR REPLACE FUNCTION pg_temp.citus_client_limit(configured_value text)
RETURNS TABLE (client_limit bigint, client_limit_status text)
LANGUAGE sql IMMUTABLE AS $limit$
  WITH parsed AS (
    SELECT CASE WHEN btrim(configured_value) ~ '^-?[0-9]{1,10}$'
                THEN btrim(configured_value)::bigint END AS value
  )
  SELECT CASE WHEN value BETWEEN 0 AND 2147483647 THEN value END,
         CASE WHEN value = -1 THEN 'disabled'
              WHEN value = 0 THEN 'blocked'
              WHEN value BETWEEN 1 AND 2147483647 THEN 'limited'
              ELSE 'unknown' END
  FROM parsed
$limit$;