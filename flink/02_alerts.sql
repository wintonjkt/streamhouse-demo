-- Statement 2: raise an alert on bank.alerts when a customer-state record reaches HIGH risk.
-- Reads the upsert topic bank.customer-state written by 01_customer_state.sql.
-- Submit with flinkConfiguration {"table.exec.source.idle-timeout": "1 s"} like statement 1.
INSERT INTO `bank.alerts`
SELECT
  `key`,
  CAST(JSON_OBJECT(
    'alert_type' VALUE 'HIGH_RISK_CUSTOMER',
    'customer_id' VALUE JSON_VALUE(s, '$.customer_id'),
    'run_id' VALUE JSON_VALUE(s, '$.run_id'),
    'name' VALUE JSON_VALUE(s, '$.name'),
    'as_of' VALUE JSON_VALUE(s, '$.as_of'),
    'customer_risk' VALUE JSON_VALUE(s, '$.customer_risk'),
    'risk_reason' VALUE JSON_VALUE(s, '$.risk_reason'),
    'current_balance' VALUE JSON_VALUE(s, '$.current_balance'),
    'external_transfer_24h' VALUE JSON_VALUE(s, '$.external_transfer_24h'),
    'next_step' VALUE 'Agent recommends next-best action; relationship manager approval required'
  ) AS BYTES) AS `val`
FROM (SELECT `key`, CAST(`val` AS STRING) AS s FROM `bank.customer-state`)
WHERE JSON_VALUE(s, '$.customer_risk') = 'HIGH'
