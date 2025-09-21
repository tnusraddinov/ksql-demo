# ksql-demo
## loyalty program using ksql stream processing 

# Run docker compose file
```sh
docker compose -f docker-compose.yml -p ksqldb_demo up -d

```

# Create streams & tables 

```sql 

SET 'auto.offset.reset'='earliest';

CREATE STREAM customers_src (
  customer_id STRING KEY,
  name        STRING,
  segment     STRING
) WITH (
  KAFKA_TOPIC='customers_src',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
);

-- Materialize a queryable customers TABLE (pull queries)
CREATE TABLE customers AS
SELECT customer_id,
       LATEST_BY_OFFSET(name)    AS name,
       LATEST_BY_OFFSET(segment) AS segment
FROM customers_src
GROUP BY customer_id
EMIT CHANGES;


CREATE STREAM transactions_src (
  tran_id     STRING,
  customer_id STRING KEY,
  amount      DECIMAL(9,2),
  tran_type   STRING,
  category    STRING,
  event_time  STRING
) WITH (
  KAFKA_TOPIC='transactions_src',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
);

CREATE STREAM transactions (
  tran_id     STRING,
  customer_id STRING KEY,
  amount      DECIMAL(9,2),
  tran_type   STRING,
  category    STRING,
  event_time  STRING
) WITH (
  KAFKA_TOPIC='transactions_src',
  VALUE_FORMAT='JSON',
  TIMESTAMP='event_time',
  TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss[.SSS][.SSSSSS][X][XXX][XXXXX]',
  PARTITIONS=1
);


INSERT INTO customers_src(customer_id, name, segment) VALUES ('101','Customer1','gold');
INSERT INTO customers_src(customer_id, name, segment) VALUES ('102','Customer2','silver');

INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',40.00,'purchase','electronics','2025-09-10T10:00:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',50.00,'purchase','books',      '2025-09-10T10:02:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',60.00,'purchase','grocery',    '2025-09-10T10:04:00Z');

INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'102',50.00,'purchase','electronics','2025-09-10T10:10:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'102',60.00,'purchase','books',      '2025-09-10T10:12:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'102',70.00,'purchase','grocery',    '2025-09-10T10:14:00Z');

INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',40.00,'purchase','electronics','2025-09-10T11:00:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',50.00,'purchase','books',      '2025-09-10T11:02:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',60.00,'purchase','grocery',    '2025-09-10T11:04:00Z');
```


# DEMO 1 - ⚡ “Flash Shopper” (Session window)

- Business rule: If a customer makes ≥ 3 purchases within a session (no idle gap > 5 minutes) and spends ≥ 100 in that session → award bonus.
```sql
CREATE TABLE reward_flash_shopper
WITH (KAFKA_TOPIC='reward_flash_shopper', VALUE_FORMAT='JSON', RETENTION_MS = '2592000000') AS
SELECT
    customer_id,
    TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd''T''HH:mm:ss[.SSS]') AS session_start,
    TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd''T''HH:mm:ss[.SSS]')   AS session_end,
    COUNT(*)                                                        AS orders_in_session,
    CAST(SUM(amount) AS DECIMAL(12,2))                              AS spend_in_session
FROM transactions
WINDOW SESSION (5 MINUTES, GRACE PERIOD 15 MINUTES)
WHERE tran_type = 'purchase'
GROUP BY customer_id
HAVING COUNT(*) >= 3 AND SUM(amount) >= 100
EMIT CHANGES;

select * from reward_flash_shopper emit changes;

```



# DEMO 2 - 🏅 “Daily High-Roller” (Tumbling window)
## High spending per customer per day
- Business rule:In each calendar day (UTC), if a customer’s purchase spend ≥ 500, grant a tiered bonus based on segment (gold, silver).

```sql
CREATE TABLE daily_high_roller AS
SELECT
    customer_id,
    TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd''T''HH:mm:ss[.SSS]') AS day_start,
    TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd''T''HH:mm:ss[.SSS]')   AS day_end,
    SUM(amount) AS total_spend
FROM transactions
WINDOW TUMBLING (SIZE 1 DAY, GRACE PERIOD 30 MINUTES)
WHERE tran_type='purchase'
GROUP BY customer_id
HAVING SUM(amount)>=500
EMIT CHANGES;

select * from daily_high_roller ;

INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',200.00,'purchase','electronics','2025-09-11T10:00:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',100.00,'purchase','books',      '2025-09-11T10:02:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',300.00,'purchase','grocery',    '2025-09-11T10:04:00Z');

select * from daily_high_roller ;

```

# DEMO 3 - “Category Explorer” (Distinct count in daily window)
- Business rule: If a customer buys from ≥ 4 distinct categories in one day → award variety bonus.
```sql
CREATE TABLE daily_category_explorer AS
SELECT
    customer_id,
    TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd''T''HH:mm:ss[.SSS]') AS day_start,
    TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd''T''HH:mm:ss[.SSS]')   AS day_end,
    COUNT_DISTINCT(category) AS distinct_categories,
    SUM(amount)              AS total_spend
FROM transactions
WINDOW TUMBLING (SIZE 1 DAY, GRACE PERIOD 30 MINUTES)
WHERE tran_type='purchase'
GROUP BY customer_id
HAVING COUNT_DISTINCT(category) >= 4
EMIT CHANGES;

select * from daily_category_explorer ;

INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',200.00,'purchase','electronics','2025-09-11T10:00:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',100.00,'purchase','books',      '2025-09-11T10:02:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',300.00,'purchase','grocery',    '2025-09-11T10:04:00Z');
INSERT INTO transactions(tran_id, customer_id, amount, tran_type, category, event_time) VALUES (uuid(),'101',400.00,'purchase','sports',    '2025-09-11T10:06:00Z');

select * from daily_category_explorer ;

```


# JOIN

```sql
CREATE STREAM transactions_customers AS
SELECT t.tran_id, t.customer_id, t.amount, t.tran_type, t.category, t.event_time, c.name, c.segment
FROM transactions_src t
LEFT JOIN customers c
  ON t.customer_id = c.customer_id
EMIT CHANGES;

select * from transactions_customers emit changes limit 5;

```
