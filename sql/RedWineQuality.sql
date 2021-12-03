-- Databricks notebook source
-- Check unique loads
SELECT DISTINCT(load_dt || " - " || load_source || " - " || load_component) 
    --   , DISTINCT(load_source), DISTINCT(load_component)
  FROM ggw_wine.quality_red_wine
;


-- COMMAND ----------

-- Simple count profile
SELECT COUNT(*)
  FROM ggw_wine.quality_red_wine
;

