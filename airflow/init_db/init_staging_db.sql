\c staging_db;

CREATE TABLE IF NOT EXISTS opinion_data (
            year_month VARCHAR(7),
            type VARCHAR(50),
            positive_opinion INT,
            neutral_opinion INT,
            negative_opinion INT,
            country VARCHAR(50)
        );

CREATE TABLE IF NOT EXISTS energy_aggregated_data (
            year_month VARCHAR(7),
            type VARCHAR(50),
            energy_production FLOAT DEFAULT 0.0,
            energy_emission FLOAT DEFAULT 0.0,
            country VARCHAR(50)
        );

CREATE TABLE IF NOT EXISTS energy_france_data (
            year_month VARCHAR(7),
            type VARCHAR(50),
            energy_production FLOAT DEFAULT 0.0,
            energy_emission FLOAT DEFAULT 0.0,
            country VARCHAR(50)
        );

CREATE TABLE IF NOT EXISTS energy_germany_data (
            year_month VARCHAR(7),
            type VARCHAR(50),
            energy_production FLOAT DEFAULT 0.0,
            energy_emission FLOAT DEFAULT 0.0,
            country VARCHAR(50)
        );

CREATE TABLE IF NOT EXISTS energy_enriched_data (
            year_month VARCHAR(7),
            type VARCHAR(50),
            energy_production FLOAT DEFAULT 0.0,
            energy_emission FLOAT DEFAULT 0.0,
            country VARCHAR(50)
        );

CREATE TABLE IF NOT EXISTS energy_opinion_data (
            year_month VARCHAR(7),
            type VARCHAR(50),
            energy_production FLOAT DEFAULT 0.0,
            energy_emission FLOAT DEFAULT 0.0,
            positive_opinion INT DEFAULT 0,
            neutral_opinion INT DEFAULT 0,
            negative_opinion INT DEFAULT 0,
            country VARCHAR(50)
        );


