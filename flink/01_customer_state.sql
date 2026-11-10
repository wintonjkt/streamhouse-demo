-- Flink SQL for the Banking Streamhouse demo (CMF / Flink 1.19, catalog bank-catalog, database bank-db).
-- The bank.* topics are exposed as (key VARBINARY, val VARBINARY) tables, so JSON is parsed with JSON_VALUE.
-- NOTE: demo thresholds, not production policy.
--
-- Statement 1: enrich transactions + digital events with Db2 reference data and write bank.customer-state.
-- All aggregates share one OVER window (Flink requirement): per customer and per scenario run, 24 hours.

INSERT INTO `bank.customer-state`
SELECT
  CAST(e.customer_id AS BYTES) AS `key`,
  CAST(JSON_OBJECT(
    'customer_id' VALUE e.customer_id,
    'run_id' VALUE e.run_id,
    'name' VALUE c.name,
    'segment' VALUE c.segment,
    'as_of' VALUE CAST(e.rt AS STRING),
    'last_event' VALUE e.kind,
    'opening_savings_balance' VALUE a.balance,
    'current_balance' VALUE a.balance + e.delta,
    'avg_balance' VALUE c.avg_balance,
    'normal_external_transfer' VALUE c.normal_external_transfer,
    'external_transfer_24h' VALUE e.ext24,
    'external_transfer_vs_normal' VALUE ROUND(e.ext24 / c.normal_external_transfer, 1),
    'recent_product_search' VALUE CASE WHEN e.searched = 1 THEN 'time deposit rate' ELSE CAST(NULL AS STRING) END,
    'credit_card_activity' VALUE CASE WHEN e.card = 1 THEN 'declining' ELSE 'normal' END,
    'engagement' VALUE c.engagement,
    'products_owned' VALUE c.products_owned,
    'customer_risk' VALUE CASE
        WHEN e.ext24 > 100000000 AND e.searched = 1 THEN 'HIGH'
        WHEN e.ext24 > 50000000 THEN 'MEDIUM'
        ELSE 'LOW' END,
    'risk_reason' VALUE CASE
        WHEN e.ext24 > 100000000 AND e.searched = 1 THEN 'External transfers exceed IDR 100M and the customer searched for time deposit rates'
        WHEN e.ext24 > 50000000 THEN 'External transfers exceed IDR 50M versus a normal level of IDR 10M'
        ELSE 'Behavior within normal range' END
  ) AS BYTES) AS `val`
FROM (
  SELECT customer_id, run_id, kind, rt,
         SUM(savings_delta) OVER w AS delta,
         SUM(ext_out) OVER w AS ext24,
         MAX(search_flag) OVER w AS searched,
         MAX(card_flag) OVER w AS card
  FROM (
    SELECT JSON_VALUE(s, '$.customer_id') AS customer_id,
           COALESCE(JSON_VALUE(s, '$.run_id'), 'adhoc') AS run_id,
           'TXN' AS kind,
           CASE WHEN JSON_VALUE(s, '$.account_id') LIKE '%-SAV'
                THEN (CASE WHEN JSON_VALUE(s, '$.direction') = 'IN' THEN 1.0 ELSE -1.0 END)
                     * CAST(JSON_VALUE(s, '$.amount') AS DOUBLE)
                ELSE 0.0 END AS savings_delta,
           CASE WHEN JSON_VALUE(s, '$.channel') = 'EXTERNAL' AND JSON_VALUE(s, '$.direction') = 'OUT'
                THEN CAST(JSON_VALUE(s, '$.amount') AS DOUBLE) ELSE 0.0 END AS ext_out,
           0 AS search_flag, 0 AS card_flag, rt
    FROM (SELECT CAST(`val` AS STRING) AS s, `$rowtime` AS rt FROM `bank.transactions`)
    UNION ALL
    SELECT JSON_VALUE(s, '$.customer_id'),
           COALESCE(JSON_VALUE(s, '$.run_id'), 'adhoc'),
           'DIG', 0.0, 0.0,
           CASE WHEN JSON_VALUE(s, '$.event_type') = 'PRODUCT_SEARCH' THEN 1 ELSE 0 END,
           CASE WHEN JSON_VALUE(s, '$.event_type') = 'CARD_USAGE_CHANGE' THEN 1 ELSE 0 END,
           rt
    FROM (SELECT CAST(`val` AS STRING) AS s, `$rowtime` AS rt FROM `bank.digital-events`)
  )
  WINDOW w AS (PARTITION BY customer_id, run_id ORDER BY rt
               RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW)
) e
JOIN (
  SELECT JSON_VALUE(s, '$.customer_id') AS customer_id,
         JSON_VALUE(s, '$.name') AS name,
         JSON_VALUE(s, '$.segment') AS segment,
         CAST(JSON_VALUE(s, '$.avg_balance') AS DOUBLE) AS avg_balance,
         CAST(JSON_VALUE(s, '$.normal_external_transfer') AS DOUBLE) AS normal_external_transfer,
         JSON_VALUE(s, '$.engagement_level') AS engagement,
         CAST(JSON_VALUE(s, '$.products_owned') AS INT) AS products_owned
  FROM (SELECT CAST(`val` AS STRING) AS s FROM `bank.customer`)
) c ON e.customer_id = c.customer_id
JOIN (
  SELECT JSON_VALUE(s, '$.customer_id') AS customer_id,
         CAST(JSON_VALUE(s, '$.balance') AS DOUBLE) AS balance
  FROM (SELECT CAST(`val` AS STRING) AS s FROM `bank.accounts`)
  WHERE JSON_VALUE(s, '$.account_type') = 'SAVINGS'
) a ON e.customer_id = a.customer_id
