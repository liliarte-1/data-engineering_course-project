-- secure schema
IF SCHEMA_ID('dw') IS NULL
    EXEC ('CREATE SCHEMA dw');
GO

/*
dims 
*/

CREATE TABLE dw.dim_autonomy (
    CODAUTO       INT           NOT NULL,
    CODAUTO_NAME  NVARCHAR(200)  NOT NULL,
    CONSTRAINT PK_dim_autonomy PRIMARY KEY (CODAUTO)
);
GO

CREATE TABLE dw.dim_province (
    CPRO      INT           NOT NULL,
    CODAUTO   INT           NOT NULL,
    CPRO_NAME NVARCHAR(200) NOT NULL,
    CONSTRAINT PK_dim_province PRIMARY KEY (CPRO),
    CONSTRAINT FK_dim_province_autonomy
        FOREIGN KEY (CODAUTO) REFERENCES dw.dim_autonomy (CODAUTO)
);
GO

CREATE TABLE dw.dim_time (
    [YEAR] INT NOT NULL,
    CONSTRAINT PK_dim_time PRIMARY KEY ([YEAR])
);
GO

CREATE TABLE dw.dim_sex (
    SEX NVARCHAR(50) NOT NULL,
    CONSTRAINT PK_dim_sex PRIMARY KEY (SEX)
);
GO

CREATE TABLE dw.dim_death_cause (
    DEATH_CAUSE_CODE NVARCHAR(200) NOT NULL,
    DEATH_CAUSE_NAME NVARCHAR(300) NOT NULL,
    CONSTRAINT PK_dim_death_cause PRIMARY KEY (DEATH_CAUSE_CODE)
);
GO

CREATE TABLE dw.dim_economic_sector (
    ECONOMIC_SECTOR NVARCHAR(200) NOT NULL,
    CONSTRAINT PK_dim_economic_sector PRIMARY KEY (ECONOMIC_SECTOR)
);
GO

-- PK composed: (CPRO, MUN_NUMBER)
CREATE TABLE dw.dim_municipality (
    CPRO       INT           NOT NULL,
    MUN_NUMBER INT           NOT NULL,
    MUN_NAME   NVARCHAR(200) NOT NULL,
    CONSTRAINT PK_dim_municipality PRIMARY KEY (CPRO, MUN_NUMBER),
    CONSTRAINT FK_dim_municipality_province
        FOREIGN KEY (CPRO) REFERENCES dw.dim_province (CPRO)
);
GO

/* 
facts
*/


CREATE TABLE dw.fact_deaths (
    CPRO             INT          NOT NULL,
    [YEAR]           INT          NOT NULL,
    SEX              NVARCHAR(50) NOT NULL,
    DEATH_CAUSE_CODE NVARCHAR(200) NOT NULL,
    TOTAL_DEATHS     INT          NOT NULL,
    CONSTRAINT PK_fact_deaths PRIMARY KEY (CPRO, [YEAR], SEX, DEATH_CAUSE_CODE),
    CONSTRAINT FK_fact_deaths_province
        FOREIGN KEY (CPRO) REFERENCES dw.dim_province (CPRO),
    CONSTRAINT FK_fact_deaths_time
        FOREIGN KEY ([YEAR]) REFERENCES dw.dim_time ([YEAR]),
    CONSTRAINT FK_fact_deaths_sex
        FOREIGN KEY (SEX) REFERENCES dw.dim_sex (SEX),
    CONSTRAINT FK_fact_deaths_cause
        FOREIGN KEY (DEATH_CAUSE_CODE) REFERENCES dw.dim_death_cause (DEATH_CAUSE_CODE)
);
GO

CREATE TABLE dw.fact_economic_sector (
    CPRO            INT           NOT NULL,
    [YEAR]          INT           NOT NULL,
    ECONOMIC_SECTOR NVARCHAR(200) NOT NULL,
    TOTAL_VALUE     FLOAT         NOT NULL,  
    CONSTRAINT PK_fact_economic_sector PRIMARY KEY (CPRO, [YEAR], ECONOMIC_SECTOR),
    CONSTRAINT FK_fact_economic_sector_province
        FOREIGN KEY (CPRO) REFERENCES dw.dim_province (CPRO),
    CONSTRAINT FK_fact_economic_sector_time
        FOREIGN KEY ([YEAR]) REFERENCES dw.dim_time ([YEAR]),
    CONSTRAINT FK_fact_economic_sector_sector
        FOREIGN KEY (ECONOMIC_SECTOR) REFERENCES dw.dim_economic_sector (ECONOMIC_SECTOR)
);
GO


CREATE TABLE dw.fact_population_municipality (
    CPRO        INT NOT NULL,
    MUN_NUMBER  INT NOT NULL,
    [YEAR]      INT NOT NULL,
    POPULATION_TOTAL INT NOT NULL,
    MALE_TOTAL       INT NOT NULL,
    FEMALE_TOTAL     INT NOT NULL,
    CONSTRAINT PK_fact_population_municipality PRIMARY KEY (CPRO, MUN_NUMBER, [YEAR]),
    -- FK to municipality (KEY)
    CONSTRAINT FK_fact_population_municipality_mun
        FOREIGN KEY (CPRO, MUN_NUMBER) REFERENCES dw.dim_municipality (CPRO, MUN_NUMBER),
    CONSTRAINT FK_fact_population_municipality_time
        FOREIGN KEY ([YEAR]) REFERENCES dw.dim_time ([YEAR])
);
GO
