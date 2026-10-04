-- The warehouse has three layers (schemas):
--   staging : source data, as loaded from the file, plus load information
--   core    : the main model (dimension tables and the fact table)
--   mart    : summary tables for reports and analysis
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
