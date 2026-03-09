-- ============================================================
-- Distributed Financial Data Mesh — BigQuery Analytics Views
-- Quantitative Research & ML Feature Extraction Layer
-- ============================================================
-- Replace `your-gcp-project.financial_data_mesh` with your values.
-- Run each CREATE OR REPLACE VIEW statement individually in BQ console
-- or via the deploy_views.py script.
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- VIEW 1: latest_snapshot
-- Most recent trading day data for all tickers
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `your-gcp-project.financial_data_mesh.vw_latest_snapshot` AS
SELECT
  ticker,
  date,
  adj_close,
  adj_volume,
  rsi,
  macd_line,
  macd_hist,
  sma_20,
  sma_50,
  sma_200,
  bb_pct_b,
  vol_20d,
  annualized_vol_20d,
  momentum_20d,
  momentum_60d,
  golden_cross,
  pct_from_52w_high,
  pct_from_52w_low,
  vol_zscore,
  atr_14
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
  FROM `your-gcp-project.financial_data_mesh.stock_features`
)
WHERE rn = 1;


-- ─────────────────────────────────────────────────────────────
-- VIEW 2: cross_sectional_momentum
-- Ranks tickers by 20d and 60d momentum — quant signal layer
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `your-gcp-project.financial_data_mesh.vw_cross_sectional_momentum` AS
WITH latest AS (
  SELECT * FROM `your-gcp-project.financial_data_mesh.vw_latest_snapshot`
),
ranked AS (
  SELECT
    ticker,
    date,
    adj_close,
    momentum_20d,
    momentum_60d,
    rsi,
    vol_20d,
    PERCENT_RANK() OVER (ORDER BY momentum_20d DESC)  AS momentum_20d_pctrank,
    PERCENT_RANK() OVER (ORDER BY momentum_60d DESC)  AS momentum_60d_pctrank,
    PERCENT_RANK() OVER (ORDER BY annualized_vol_20d ASC) AS vol_pctrank,
    -- Composite momentum score (60% weight on 60d, 40% on 20d)
    0.4 * PERCENT_RANK() OVER (ORDER BY momentum_20d DESC) +
    0.6 * PERCENT_RANK() OVER (ORDER BY momentum_60d DESC) AS composite_momentum_score
  FROM latest
)
SELECT
  *,
  CASE
    WHEN composite_momentum_score >= 0.8 THEN 'STRONG_BUY'
    WHEN composite_momentum_score >= 0.6 THEN 'BUY'
    WHEN composite_momentum_score <= 0.2 THEN 'STRONG_SELL'
    WHEN composite_momentum_score <= 0.4 THEN 'SELL'
    ELSE 'NEUTRAL'
  END AS momentum_signal
FROM ranked
ORDER BY composite_momentum_score DESC;


-- ─────────────────────────────────────────────────────────────
-- VIEW 3: rsi_signal_board
-- RSI-based overbought/oversold classification
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `your-gcp-project.financial_data_mesh.vw_rsi_signals` AS
SELECT
  ticker,
  date,
  adj_close,
  rsi,
  CASE
    WHEN rsi >= 80 THEN 'EXTREMELY_OVERBOUGHT'
    WHEN rsi >= 70 THEN 'OVERBOUGHT'
    WHEN rsi <= 20 THEN 'EXTREMELY_OVERSOLD'
    WHEN rsi <= 30 THEN 'OVERSOLD'
    ELSE 'NEUTRAL'
  END AS rsi_signal,
  macd_line,
  macd_hist,
  CASE
    WHEN macd_hist > 0 AND LAG(macd_hist) OVER (PARTITION BY ticker ORDER BY date) <= 0
      THEN 'BULLISH_CROSSOVER'
    WHEN macd_hist < 0 AND LAG(macd_hist) OVER (PARTITION BY ticker ORDER BY date) >= 0
      THEN 'BEARISH_CROSSOVER'
    ELSE 'NO_CROSSOVER'
  END AS macd_signal
FROM `your-gcp-project.financial_data_mesh.stock_features`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);


-- ─────────────────────────────────────────────────────────────
-- VIEW 4: volatility_regime
-- Vol clustering for regime detection (low / normal / high / crisis)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `your-gcp-project.financial_data_mesh.vw_volatility_regime` AS
WITH vol_stats AS (
  SELECT
    ticker,
    AVG(annualized_vol_20d)    AS mean_vol,
    STDDEV(annualized_vol_20d) AS std_vol
  FROM `your-gcp-project.financial_data_mesh.stock_features`
  GROUP BY ticker
)
SELECT
  f.ticker,
  f.date,
  f.adj_close,
  f.annualized_vol_20d,
  f.atr_14,
  v.mean_vol,
  v.std_vol,
  (f.annualized_vol_20d - v.mean_vol) / NULLIF(v.std_vol, 0) AS vol_z_score,
  CASE
    WHEN f.annualized_vol_20d > v.mean_vol + 2 * v.std_vol THEN 'CRISIS'
    WHEN f.annualized_vol_20d > v.mean_vol + 1 * v.std_vol THEN 'HIGH'
    WHEN f.annualized_vol_20d < v.mean_vol - 1 * v.std_vol THEN 'LOW'
    ELSE 'NORMAL'
  END AS vol_regime
FROM `your-gcp-project.financial_data_mesh.stock_features` f
JOIN vol_stats v USING (ticker)
WHERE f.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY);


-- ─────────────────────────────────────────────────────────────
-- VIEW 5: ml_feature_set
-- Flattened, clean feature matrix for ML model ingestion
-- Includes label: next-day return direction (1 = up, 0 = down)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `your-gcp-project.financial_data_mesh.vw_ml_feature_set` AS
WITH base AS (
  SELECT
    *,
    LEAD(adj_close, 1) OVER (PARTITION BY ticker ORDER BY date) AS next_close
  FROM `your-gcp-project.financial_data_mesh.stock_features`
)
SELECT
  -- Identifiers
  ticker,
  date,

  -- Target variable
  CASE WHEN next_close > adj_close THEN 1 ELSE 0 END  AS label_next_day_up,
  SAFE_DIVIDE(next_close - adj_close, adj_close)       AS label_next_day_return,

  -- Price features
  adj_close,
  log_return,
  momentum_20d,
  momentum_60d,
  pct_from_52w_high,
  pct_from_52w_low,

  -- Moving average features
  SAFE_DIVIDE(adj_close, sma_20)   - 1 AS price_to_sma20,
  SAFE_DIVIDE(adj_close, sma_50)   - 1 AS price_to_sma50,
  SAFE_DIVIDE(adj_close, sma_200)  - 1 AS price_to_sma200,
  SAFE_DIVIDE(sma_50,    sma_200)  - 1 AS sma50_to_sma200,

  -- Oscillators
  rsi,
  macd_line,
  macd_hist,
  bb_pct_b,
  bb_bandwidth,

  -- Volatility
  vol_20d,
  annualized_vol_20d,
  atr_14,

  -- Volume
  vol_zscore,
  obv,

  -- Binary signals
  golden_cross,

  -- Metadata
  year,
  month

FROM base
WHERE
  next_close IS NOT NULL
  AND rsi IS NOT NULL
  AND sma_200 IS NOT NULL;


-- ─────────────────────────────────────────────────────────────
-- VIEW 6: daily_pipeline_audit
-- Pipeline health and data quality monitoring
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `your-gcp-project.financial_data_mesh.vw_pipeline_audit` AS
SELECT
  pipeline_run,
  COUNT(DISTINCT ticker)                        AS tickers_loaded,
  COUNT(*)                                      AS total_rows,
  MIN(date)                                     AS earliest_date,
  MAX(date)                                     AS latest_date,
  COUNTIF(rsi IS NULL)                          AS null_rsi_count,
  COUNTIF(sma_200 IS NULL)                      AS null_sma200_count,
  COUNTIF(adj_close <= 0)                       AS invalid_price_count,
  MAX(processed_at)                             AS last_processed_at
FROM `your-gcp-project.financial_data_mesh.stock_features`
GROUP BY pipeline_run
ORDER BY pipeline_run DESC;
