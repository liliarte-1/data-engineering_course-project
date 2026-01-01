/* =========================
   0) Schema lógico
========================= */
CREATE SCHEMA dw;
GO

/* =========================
   1) DIMENSIONES
========================= */

/* Tiempo (en tu caso: solo año) */
CREATE TABLE dw.dim_time (
    [year] INT NOT NULL,
    CONSTRAINT PK_dim_time PRIMARY KEY ([year])
);
GO

/* Indicador (qué mide el value) */
CREATE TABLE dw.dim_indicator (
    indicator_code  VARCHAR(80)  NOT NULL,   -- ej: 'population', 'households'
    indicator_name  VARCHAR(200) NOT NULL,   -- ej: 'Population'
    unit            VARCHAR(50)  NULL,       -- ej: 'persons', 'households'
    topic           VARCHAR(120) NULL,       -- ej: 'demography'
    CONSTRAINT PK_dim_indicator PRIMARY KEY (indicator_code)
);
GO

/* Dataset origen (trazabilidad) */
CREATE TABLE dw.dim_source_dataset (
    dataset_code VARCHAR(80)  NOT NULL,      -- ej: 'population_municipalities_csv'
    dataset_name VARCHAR(200) NOT NULL,      -- ej: 'Population by municipalities'
    source_org   VARCHAR(200) NULL,          -- ej: 'INE'
    source_url   VARCHAR(400) NULL,
    version_tag  VARCHAR(80)  NULL,          -- opcional: fecha descarga, versión, etc.
    CONSTRAINT PK_dim_source_dataset PRIMARY KEY (dataset_code)
);
GO

/* Geografía unificada (España / provincia / municipio) */
CREATE TABLE dw.dim_geo (
    geo_code      VARCHAR(20)  NOT NULL,     -- tu código natural (INE / id / etc.)
    geo_name      VARCHAR(200) NOT NULL,
    geo_level     VARCHAR(12)  NOT NULL,     -- 'country' | 'province' | 'municipality'
    province_code VARCHAR(20)  NULL,         -- si es municipality, aquí guardas su provincia (opcional)
    CONSTRAINT PK_dim_geo PRIMARY KEY (geo_code),
    CONSTRAINT CK_dim_geo_level CHECK (geo_level IN ('country','province','municipality'))
);
GO

/* (Opcional) Si QUIERES forzar integridad de province_code:
   OJO: esto es una “relación” pero NO te obliga a montar jerarquías complejas.
   Si no lo quieres, no ejecutes este ALTER.

ALTER TABLE dw.dim_geo
ADD CONSTRAINT FK_dim_geo_province
FOREIGN KEY (province_code) REFERENCES dw.dim_geo(geo_code);
GO
*/

/* =========================
   2) HECHOS (fact unificada)
========================= */

CREATE TABLE dw.fact_indicator (
    [year]         INT          NOT NULL,
    geo_code       VARCHAR(20)  NOT NULL,
    indicator_code VARCHAR(80)  NOT NULL,
    dataset_code   VARCHAR(80)  NOT NULL,
    value          DECIMAL(18,4) NOT NULL,

    -- PK compuesta: 1 valor por (año, lugar, indicador, dataset)
    CONSTRAINT PK_fact_indicator
        PRIMARY KEY ([year], geo_code, indicator_code, dataset_code),

    CONSTRAINT FK_fact_time
        FOREIGN KEY ([year]) REFERENCES dw.dim_time([year]),

    CONSTRAINT FK_fact_geo
        FOREIGN KEY (geo_code) REFERENCES dw.dim_geo(geo_code),

    CONSTRAINT FK_fact_indicator
        FOREIGN KEY (indicator_code) REFERENCES dw.dim_indicator(indicator_code),

    CONSTRAINT FK_fact_dataset
        FOREIGN KEY (dataset_code) REFERENCES dw.dim_source_dataset(dataset_code)
);
GO

/* Índices útiles para consultas típicas */
CREATE INDEX IX_fact_indicator_indicator_year
    ON dw.fact_indicator(indicator_code, [year]);
GO

CREATE INDEX IX_fact_indicator_geo_year
    ON dw.fact_indicator(geo_code, [year]);
GO
