-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 01 - Setup e Extração (Bronze SQL)
-- MAGIC
-- MAGIC **Objetivo:** usar SQL Warehouse Serverless (2X-Small) para ler CSVs no Volume Unity Catalog e criar tabelas Bronze.

 -- COMMAND ----------

-- 1) Criar schema da trilha
CREATE SCHEMA IF NOT EXISTS training_sql_serverless;
USE training_sql_serverless;

-- COMMAND ----------

-- 1.1) Criar volume de entrada para os arquivos CSV
CREATE VOLUME IF NOT EXISTS workspace.training_sql_serverless.raw_files;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extração de pedidos (Bronze)

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_orders_raw AS
SELECT
  order_id,
  customer_id,
  product_category,
  product_name,
  quantity,
  unit_price,
  order_date,
  region,
  status,
  current_timestamp() AS _ingestion_timestamp,
  input_file_name() AS _source_file
FROM read_files(
  'dbfs:/Volumes/workspace/training_sql_serverless/raw_files/orders.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extração de clientes (Bronze)

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_customers_raw AS
SELECT
  customer_id,
  name,
  email,
  city,
  signup_date,
  current_timestamp() AS _ingestion_timestamp,
  'dbfs:/Volumes/workspace/training_sql_serverless/raw_files/customers.csv' AS _source_file
FROM read_files(
  'dbfs:/Volumes/workspace/training_sql_serverless/raw_files/customers.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validações da camada Bronze

-- COMMAND ----------

-- 1) Contagem de registros carregados
SELECT 'bronze_orders_raw' AS table_name, COUNT(*) AS record_count
FROM bronze_orders_raw
UNION ALL
SELECT 'bronze_customers_raw' AS table_name, COUNT(*) AS record_count
FROM bronze_customers_raw;

-- COMMAND ----------

-- 2) Reconciliação entre arquivos de origem e tabelas Bronze
WITH source_counts AS (
  SELECT 'orders' AS dataset, COUNT(*) AS source_record_count
  FROM read_files(
    'dbfs:/Volumes/workspace/training_sql_serverless/raw_files/orders.csv',
    format => 'csv',
    header => true,
    inferSchema => true
  )
  UNION ALL
  SELECT 'customers' AS dataset, COUNT(*) AS source_record_count
  FROM read_files(
    'dbfs:/Volumes/workspace/training_sql_serverless/raw_files/customers.csv',
    format => 'csv',
    header => true,
    inferSchema => true
  )
),
bronze_counts AS (
  SELECT 'orders' AS dataset, COUNT(*) AS bronze_record_count
  FROM bronze_orders_raw
  UNION ALL
  SELECT 'customers' AS dataset, COUNT(*) AS bronze_record_count
  FROM bronze_customers_raw
)
SELECT
  source_counts.dataset,
  source_counts.source_record_count,
  bronze_counts.bronze_record_count,
  source_counts.source_record_count - bronze_counts.bronze_record_count
    AS record_count_difference
FROM source_counts
INNER JOIN bronze_counts
  ON source_counts.dataset = bronze_counts.dataset;

-- COMMAND ----------

-- 3) Chaves obrigatórias e duplicidade nas tabelas Bronze
SELECT 'orders_without_order_id' AS validation_name, COUNT(*) AS invalid_record_count
FROM bronze_orders_raw
WHERE order_id IS NULL OR trim(order_id) = ''
UNION ALL
SELECT 'orders_without_customer_id' AS validation_name, COUNT(*) AS invalid_record_count
FROM bronze_orders_raw
WHERE customer_id IS NULL OR trim(customer_id) = ''
UNION ALL
SELECT 'duplicate_order_ids' AS validation_name, COUNT(*) AS invalid_record_count
FROM (
  SELECT order_id
  FROM bronze_orders_raw
  GROUP BY order_id
  HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'customers_without_customer_id' AS validation_name, COUNT(*) AS invalid_record_count
FROM bronze_customers_raw
WHERE customer_id IS NULL OR trim(customer_id) = ''
UNION ALL
SELECT 'duplicate_customer_ids' AS validation_name, COUNT(*) AS invalid_record_count
FROM (
  SELECT customer_id
  FROM bronze_customers_raw
  GROUP BY customer_id
  HAVING COUNT(*) > 1
);

-- COMMAND ----------

-- 4) Valores fora do domínio permitido de status
SELECT
  coalesce(status, '<NULL>') AS invalid_status,
  COUNT(*) AS record_count
FROM bronze_orders_raw
WHERE status IS NULL
   OR lower(trim(status)) NOT IN ('completed', 'cancelled', 'pending')
GROUP BY coalesce(status, '<NULL>')
ORDER BY record_count DESC;

-- COMMAND ----------

-- 5) Datas de pedidos nulas, inválidas ou futuras
SELECT
  COUNT(*) AS total_orders,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date_count,
  SUM(
    CASE
      WHEN order_date IS NOT NULL
       AND try_cast(order_date AS TIMESTAMP) IS NULL
      THEN 1
      ELSE 0
    END
  ) AS invalid_order_date_count,
  SUM(
    CASE
      WHEN try_cast(order_date AS TIMESTAMP) > current_timestamp() THEN 1
      ELSE 0
    END
  ) AS future_order_date_count
FROM bronze_orders_raw;

-- COMMAND ----------

-- 6) Pedidos sem cliente correspondente na tabela Bronze de clientes
SELECT COUNT(*) AS orders_without_matching_customer
FROM bronze_orders_raw AS orders
LEFT ANTI JOIN bronze_customers_raw AS customers
  ON orders.customer_id = customers.customer_id;


