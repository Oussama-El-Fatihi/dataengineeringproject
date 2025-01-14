\c energy_db;

-- Table: Energy_Type
CREATE TABLE IF NOT EXISTS Energy_Type_Dim (
    energy_type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,
    renewable_flag BOOLEAN
);

-- Table: Country
CREATE TABLE IF NOT EXISTS Country_Dim (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(50) NOT NULL UNIQUE
);

-- Table: Time
CREATE TABLE Time_Dim (
    time_id SERIAL PRIMARY KEY,
    month INT NOT NULL,
    year INT NOT NULL,
    CONSTRAINT unique_month_year UNIQUE(month, year)
);

-- Table: Source
CREATE TABLE IF NOT EXISTS Source_Dim (
    source_id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL UNIQUE
);

-- Fact Table: Energy_Impact
CREATE TABLE IF NOT EXISTS Energy_Impact (
    energy_type_id INT REFERENCES Energy_Type_Dim(energy_type_id),
    country_id INT REFERENCES Country_Dim(country_id),
    time_id INT REFERENCES Time_Dim(time_id),
    source_id INT REFERENCES Source_Dim(source_id),
    neutral_opinion INT DEFAULT 0,
    negative_opinion INT DEFAULT 0,
    positive_opinion INT DEFAULT 0,
    energy_production FLOAT DEFAULT 0.0,
    energy_emission FLOAT DEFAULT 0.0,
    energy_efficiency FLOAT DEFAULT 0.0
);