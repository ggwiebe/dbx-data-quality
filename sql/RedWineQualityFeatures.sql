-- Databricks notebook source
SELECT *
  FROM ggw_wine.quality_red_wine_features
;

-- DROP TABLE IF EXISTS ggw_wine.quality_red_wine_features;

-- DELETE FROM ggw_wine.quality_red_wine
--  WHERE load_dt != '2021-12-02 15:49:49.45855'
-- ;

SELECT *
  FROM ggw_wine.quality_red_wine_features_inferenced_2021_12_03 
;

-- DROP TABLE ggw_wine.quality_red_wine_features_infer_2021_12_03;

-- CREATE TABLE ggw_wine.quality_red_wine_features_inferenced_2021_12_03 
-- LOCATION "/users/glenn.wiebe@databricks.com/ggw_wine/ggw_wine.db/ggw_quality_red_wine_inferenced_2021_12_03"
-- ;