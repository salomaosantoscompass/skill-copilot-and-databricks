-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 02 - Limpeza e ETL (Silver SQL)
-- MAGIC
-- MAGIC **Objetivo:** limpar pedidos/clientes e criar tabela enriquecida para análise.

-- COMMAND ----------

USE CATALOG workspace;

USE training_sql_serverless;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver de pedidos: padronização, filtros e deduplicação

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_orders_clean AS
WITH orders_typed AS (
  SELECT
    CAST(TRIM(CAST(order_id AS STRING)) AS STRING) AS order_id,
    CAST(TRIM(CAST(customer_id AS STRING)) AS STRING) AS customer_id,
    LOWER(TRIM(CAST(product_category AS STRING))) AS product_category,
    TRIM(CAST(product_name AS STRING)) AS product_name,
    TRY_CAST(quantity AS INT) AS quantity,
    TRY_CAST(unit_price AS DECIMAL(18,2)) AS unit_price,
    TO_DATE(order_date) AS order_date,
    LOWER(TRIM(CAST(region AS STRING))) AS region,
    LOWER(TRIM(CAST(status AS STRING))) AS status
  FROM bronze_orders_raw
),
orders_filtered AS (
  SELECT *
  FROM orders_typed
  WHERE order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND quantity BETWEEN 1 AND 100
    AND unit_price BETWEEN 0.01 AND 50000
    AND status = 'completed'
),
orders_dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY order_date DESC NULLS LAST
    ) AS rn
  FROM orders_filtered
)
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
  ROUND(quantity * unit_price, 2) AS total_amount
FROM orders_dedup
WHERE rn = 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver de clientes: normalização e validação de email

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_customers_clean AS
SELECT
  CAST(customer_id AS STRING) AS customer_id,
  TRIM(CAST(name AS STRING)) AS customer_name,
  LOWER(TRIM(CAST(email AS STRING))) AS email,
  LOWER(TRIM(CAST(city AS STRING))) AS city,
  TO_DATE(signup_date) AS signup_date,
  LOWER(TRIM(CAST(email AS STRING))) RLIKE '^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$' AS is_valid_email
FROM bronze_customers_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enriquecimento Silver (join)

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_orders_enriched AS
SELECT
  o.order_id,
  o.customer_id,
  o.product_category,
  o.product_name,
  o.quantity,
  o.unit_price,
  o.order_date,
  o.region,
  o.status,
  o.total_amount,
  c.customer_name,
  c.city AS customer_city,
  c.email,
  c.is_valid_email,
  c.signup_date
FROM silver_orders_clean o
LEFT JOIN silver_customers_clean c
  ON o.customer_id = c.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validações da camada Silver

-- COMMAND ----------

-- 1) Pedidos sem cliente após o LEFT JOIN e percentual aceitável
SELECT
  COUNT(*) AS total_orders,
  SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END)
    AS orders_without_customer,
  ROUND(
    100.0 * SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS orders_without_customer_percentage,
  CASE
    WHEN 100.0 * SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) / COUNT(*) < 2
    THEN 'PASS'
    ELSE 'FAIL'
  END AS threshold_under_2_percent
FROM silver_orders_enriched;

-- COMMAND ----------

-- 2) Integridade do cálculo de valor total
SELECT
  COUNT(*) AS total_orders,
  SUM(
    CASE
      WHEN total_amount != ROUND(quantity * unit_price, 2) THEN 1
      ELSE 0
    END
  ) AS inconsistent_total_amount_count
FROM silver_orders_enriched;

-- COMMAND ----------

-- 3) Regras de domínio preservadas na tabela enriquecida
SELECT
  COUNT(*) AS total_orders,
  SUM(CASE WHEN quantity NOT BETWEEN 1 AND 100 THEN 1 ELSE 0 END)
    AS invalid_quantity_count,
  SUM(CASE WHEN unit_price NOT BETWEEN 0.01 AND 50000 THEN 1 ELSE 0 END)
    AS invalid_unit_price_count,
  SUM(CASE WHEN status != 'completed' THEN 1 ELSE 0 END)
    AS invalid_status_count
FROM silver_orders_enriched;

-- COMMAND ----------

-- 4) Unicidade de pedido após a deduplicação
SELECT
  COUNT(*) AS duplicated_order_id_count
FROM (
  SELECT order_id
  FROM silver_orders_clean
  GROUP BY order_id
  HAVING COUNT(*) > 1
);

-- COMMAND ----------

-- 5) Qualidade dos dados cadastrais de cliente no enriquecimento
SELECT
  COUNT(*) AS total_orders,
  SUM(CASE WHEN customer_name IS NULL OR trim(customer_name) = '' THEN 1 ELSE 0 END)
    AS missing_customer_name_count,
  SUM(CASE WHEN customer_city IS NULL OR trim(customer_city) = '' THEN 1 ELSE 0 END)
    AS missing_customer_city_count,
  SUM(CASE WHEN email IS NULL OR trim(email) = '' THEN 1 ELSE 0 END)
    AS missing_email_count,
  SUM(CASE WHEN is_valid_email IS NOT TRUE THEN 1 ELSE 0 END)
    AS invalid_email_count,
  SUM(CASE WHEN signup_date IS NULL THEN 1 ELSE 0 END)
    AS missing_signup_date_count
FROM silver_orders_enriched;
 
