-- =========================
-- LIMPIEZA (opcional para pruebas)
-- =========================
DROP TABLE IF EXISTS fact_population_municipality;
DROP TABLE IF EXISTS dim_municipality;
DROP TABLE IF EXISTS dim_province;
DROP TABLE IF EXISTS dim_time;

-- =========================
-- DIMENSIONS
-- =========================

CREATE TABLE dim_time (
    year INT NOT NULL PRIMARY KEY
);

CREATE TABLE dim_province (
    province_number INT NOT NULL PRIMARY KEY,
    province_name NVARCHAR(200) NOT NULL
);

CREATE TABLE dim_municipality (
    municipality_number INT NOT NULL PRIMARY KEY,
    municipality_name NVARCHAR(200) NOT NULL,
    province_number INT NULL,
    CONSTRAINT fk_municipality_province
        FOREIGN KEY (province_number)
        REFERENCES dim_province(province_number)
);

-- =========================
-- FACTS
-- =========================

CREATE TABLE fact_population_municipality (
    municipality_number INT NOT NULL,
    year INT NOT NULL,
    sex NVARCHAR(20) NOT NULL,
    population INT NOT NULL,

    CONSTRAINT pk_fact_population PRIMARY KEY (municipality_number, year, sex),

    CONSTRAINT fk_fact_pop_municipality
        FOREIGN KEY (municipality_number)
        REFERENCES dim_municipality(municipality_number),

    CONSTRAINT fk_fact_pop_year
        FOREIGN KEY (year)
        REFERENCES dim_time(year)
);
