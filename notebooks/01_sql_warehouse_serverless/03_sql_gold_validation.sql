-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 03 - Gold SQL e Validações
-- MAGIC
-- MAGIC **Objetivo:** gerar tabelas analíticas Gold para consumo em BI.

-- COMMAND ----------

USE CATALOG workspace;
USE training_sql_serverless;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold 1: KPIs diários

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_daily_kpis AS
SELECT
  order_date,
  COUNT(DISTINCT order_id) AS orders_count,
  ROUND(SUM(total_amount), 2) AS revenue_total,
  ROUND(AVG(total_amount), 2) AS avg_ticket
FROM silver_orders_enriched
GROUP BY order_date
ORDER BY order_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold 2: ranking por categoria

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_top_categories AS
WITH category_agg AS (
  SELECT
    product_category,
    COUNT(DISTINCT order_id) AS orders_count,
    ROUND(SUM(total_amount), 2) AS revenue_total,
    ROUND(AVG(total_amount), 2) AS avg_ticket
  FROM silver_orders_enriched
  GROUP BY product_category
),
ranked AS (
  SELECT
    product_category,
    orders_count,
    revenue_total,
    avg_ticket,
    DENSE_RANK() OVER (ORDER BY revenue_total DESC) AS revenue_rank
  FROM category_agg
)
SELECT *
FROM ranked
ORDER BY revenue_rank, product_category;



-- Criar query de validação de qualidade dos dados:
--  - nulos em chaves
--  - pedidos sem cliente correspondente
--  - percentual de emails inválidos

-- COMMAND ----------

-- Validação de nulos em chaves
SELECT
  COUNT(*) AS null_keys_count
FROM silver_orders_enriched
WHERE order_id IS NULL OR customer_id IS NULL;

-- Validação de pedidos sem cliente correspondente
SELECT
  COUNT(*) AS orders_without_customer_count
FROM workspace.training_sql_serverless.silver_orders_enriched o
LEFT JOIN workspace.training_sql_serverless.silver_customers_clean c
  ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Validação de emails inválidos
SELECT
  COUNT(*) AS invalid_email_count
FROM workspace.training_sql_serverless.silver_customers_clean
WHERE email NOT LIKE '%_@__%.__%';




-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Reconciliação Gold vs Silver (Diária)
-- MAGIC **Objetivo:** Provar a fidelidade das métricas agregadas na camada Gold em relação à Silver.

-- COMMAND ----------
USE CATALOG workspace;
USE SCHEMA training_sql_serverless;

-- COMMAND ----------
WITH silver_daily AS (
  SELECT
    CAST(order_date AS DATE) AS order_date,
    COUNT(DISTINCT order_id) AS silver_orders_count,
    ROUND(SUM(total_amount), 2) AS silver_revenue_total
  FROM silver_orders_enriched
  WHERE status = 'COMPLETED'
  GROUP BY CAST(order_date AS DATE)
)
SELECT
  s.order_date,
  s.silver_orders_count,
  COALESCE(g.orders_count, 0) AS gold_orders_count,
  (s.silver_orders_count - COALESCE(g.orders_count, 0)) AS diff_orders_count,
  s.silver_revenue_total,
  COALESCE(g.revenue_total, 0.00) AS gold_revenue_total,
  ROUND(s.silver_revenue_total - COALESCE(g.revenue_total, 0.00), 2) AS diff_revenue_total
FROM silver_daily s
FULL OUTER JOIN gold_daily_kpis g
  ON s.order_date = g.order_date
ORDER BY s.order_date DESC;


-- Completude temporal da Gold diária
-- Objetivo: encontrar lacunas de datas e dias com KPI zerado inesperado.

-- COMMAND ----------
WITH gold_daily AS (
  SELECT
    CAST(order_date AS DATE) AS order_date,
    SUM(orders_count) AS orders_count,
    ROUND(SUM(revenue_total), 2) AS revenue_total
  FROM gold_daily_kpis
  GROUP BY CAST(order_date AS DATE)
)
SELECT *
FROM gold_daily
ORDER BY order_date DESC;


WITH bounds AS (
  SELECT 
    MIN(CAST(order_date AS DATE)) AS min_date,
    MAX(CAST(order_date AS DATE)) AS max_date
  FROM gold_daily_kpis
),
calendar_spine AS (
  SELECT 
    explode(sequence(min_date, max_date, INTERVAL 1 DAY)) AS calendar_date
  FROM bounds
),
gold_daily AS (
  SELECT
    CAST(order_date AS DATE) AS order_date,
    SUM(orders_count) AS orders_count,
    ROUND(SUM(revenue_total), 2) AS revenue_total
  FROM gold_daily_kpis
  GROUP BY CAST(order_date AS DATE)
)
SELECT
  c.calendar_date,
  date_format(c.calendar_date, 'EEEE') AS day_of_week,
  COALESCE(g.orders_count, 0) AS orders_count,
  COALESCE(g.revenue_total, 0.00) AS revenue_total,
  CASE 
    WHEN g.order_date IS NULL THEN 'LACUNA (Data Faltante)'
    WHEN COALESCE(g.orders_count, 0) = 0 THEN 'KPI Zerado'
    ELSE 'OK'
  END AS status_completude
FROM calendar_spine c
LEFT JOIN gold_daily g
  ON c.calendar_date = g.order_date
ORDER BY c.calendar_date DESC;

-- END OF COMPLITUDE TEMPORAL DA GOLD DIÁRIA


-- Consistência do ranking por categoria
-- Objetivo: validar se a Gold de ranking mantém a regra correta de ordenação.
  

-- COMMAND ----------
WITH category_metrics AS (
  SELECT
    product_category,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_quantity_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue
  FROM silver_orders_enriched
  GROUP BY product_category
)
SELECT
  product_category,
  total_orders,
  total_quantity_sold,
  total_revenue,
  DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
  DENSE_RANK() OVER (ORDER BY total_quantity_sold DESC) AS volume_rank,
  CASE 
    WHEN DENSE_RANK() OVER (ORDER BY total_revenue DESC) = DENSE_RANK() OVER (ORDER BY total_quantity_sold DESC) 
      THEN 'Alinhado'
    ELSE 'Divergente (Ticket Médio Impacta)'
  END AS rank_consistency
FROM category_metrics
ORDER BY revenue_rank ASC;

-- Qualidade de vínculo cliente e email (agregado para consumo BI)
-- Objetivo: transformar validações de qualidade em métricas de negócio por dia.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_daily_data_quality_kpis AS
WITH daily_orders AS (
  SELECT
    CAST(o.order_date AS DATE) AS order_date,
    o.order_id,
    o.customer_id,
    o.total_amount,
    c.customer_name,
    c.email AS customer_email
  FROM silver_orders_clean o
  LEFT JOIN silver_customers_clean c
    ON o.customer_id = c.customer_id
)
SELECT
  order_date,
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  
  -- 1. Métricas de Vínculo de Cliente (Integridade Referencial)
  COUNT(DISTINCT CASE WHEN customer_name IS NULL THEN order_id END) AS orders_with_missing_customer,
  ROUND(
    SUM(CASE WHEN customer_name IS NULL THEN total_amount ELSE 0 END), 2
  ) AS revenue_at_risk_missing_customer,
  
  -- 2. Métricas de Qualidade de E-mail (Completude e Formato)
  COUNT(DISTINCT CASE WHEN customer_email IS NULL OR TRIM(customer_email) = '' THEN order_id END) AS orders_with_missing_email,
  COUNT(DISTINCT CASE WHEN customer_email IS NOT NULL AND customer_email NOT LIKE '%@%.%' THEN order_id END) AS orders_with_invalid_email,
  
  -- 3. Taxas de Qualidade (Percentuais para consumo em BI)
  ROUND(
    (COUNT(DISTINCT CASE WHEN customer_name IS NOT NULL THEN order_id END) * 100.0) / COUNT(DISTINCT order_id), 2
  ) AS customer_match_rate_pct,
  ROUND(
    (COUNT(DISTINCT CASE WHEN customer_email IS NOT NULL AND customer_email LIKE '%@%.%' THEN order_id END) * 100.0) / COUNT(DISTINCT order_id), 2
  ) AS valid_email_rate_pct

FROM daily_orders
GROUP BY order_date;


-- COMMAND ----------
SELECT 
  order_date,
  total_orders,
  total_revenue,
  orders_with_missing_customer,
  revenue_at_risk_missing_customer,
  customer_match_rate_pct,
  valid_email_rate_pct
FROM gold_daily_data_quality_kpis
ORDER BY order_date DESC;

-- Fim da validação diária de qualidade de dados

-- anomalias

-- COMMAND ----------
CREATE OR REPLACE VIEW gold_daily_anomaly_detection AS
WITH daily_base AS (
  SELECT
    CAST(order_date AS DATE) AS order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_ticket
  FROM silver_orders_clean
  GROUP BY CAST(order_date AS DATE)
),
stats_calculated AS (
  SELECT
    order_date,
    total_orders,
    total_revenue,
    avg_ticket,
    
    -- Média e Desvio Padrão Móvel da Receita (janela de 14 dias até o dia atual)
    AVG(total_revenue) OVER (
      ORDER BY order_date 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS revenue_moving_avg,
    STDDEV_SAMP(total_revenue) OVER (
      ORDER BY order_date 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS revenue_moving_stddev,

    -- Média e Desvio Padrão Móvel do Ticket Médio
    AVG(avg_ticket) OVER (
      ORDER BY order_date 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS ticket_moving_avg,
    STDDEV_SAMP(avg_ticket) OVER (
      ORDER BY order_date 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS ticket_moving_stddev
  FROM daily_base
),
z_scores AS (
  SELECT
    order_date,
    total_orders,
    total_revenue,
    avg_ticket,
    ROUND(revenue_moving_avg, 2) AS revenue_moving_avg,
    ROUND(ticket_moving_avg, 2) AS ticket_moving_avg,
    
    -- Cálculo do Z-Score para Receita
    ROUND(
      CASE 
        WHEN COALESCE(revenue_moving_stddev, 0) = 0 THEN 0 
        ELSE (total_revenue - revenue_moving_avg) / revenue_moving_stddev 
      END, 2
    ) AS revenue_z_score,

    -- Cálculo do Z-Score para Ticket Médio
    ROUND(
      CASE 
        WHEN COALESCE(ticket_moving_stddev, 0) = 0 THEN 0 
        ELSE (avg_ticket - ticket_moving_avg) / ticket_moving_stddev 
      END, 2
    ) AS ticket_z_score
  FROM stats_calculated
)
SELECT
  order_date,
  total_orders,
  total_revenue,
  revenue_moving_avg,
  revenue_z_score,
  avg_ticket,
  ticket_moving_avg,
  ticket_z_score,
  
  -- Classificação da Anomalia baseada no Z-Score (|Z| > 2 é Alerta, |Z| > 3 é Crítico)
  CASE 
    WHEN ABS(revenue_z_score) >= 3 THEN 'CRÍTICO: Anomalia de Receita (|Z| >= 3)'
    WHEN ABS(ticket_z_score) >= 3 THEN 'CRÍTICO: Anomalia de Ticket (|Z| >= 3)'
    WHEN ABS(revenue_z_score) >= 2 THEN 'ALERTA: Desvio de Receita (|Z| >= 2)'
    WHEN ABS(ticket_z_score) >= 2 THEN 'ALERTA: Desvio de Ticket (|Z| >= 2)'
    ELSE 'OK (Dentro do Padrão)'
  END AS anomaly_status
FROM z_scores;


-- COMMAND ----------
SELECT 
  order_date,
  total_revenue,
  revenue_moving_avg,
  revenue_z_score,
  avg_ticket,
  ticket_moving_avg,
  ticket_z_score,
  anomaly_status
FROM gold_daily_anomaly_detection
WHERE anomaly_status != 'OK (Dentro do Padrão)'
ORDER BY order_date DESC;