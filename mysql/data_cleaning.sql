-- Loading Data --

-- Create a new SCHEMA named SRDBv5
DROP SCHEMA `SRDBv5`;
CREATE SCHEMA `SRDBv5`;
USE SRDBv5;

-- Create srdb_data table with column names and data types from `srdb-data_fields_V5.txt`
#DROP TABLE srdb_data;
CREATE TABLE srdb_data (
	Record_number INTEGER,
	Entry_date DATE,
	Study_number INTEGER,
	Author TEXT,
	Duplicate_record TEXT, 
	Quality_flag TEXT,
	Contributor TEXT,
	Country TEXT,
	Region TEXT,
	Site_name TEXT,
	Site_ID VARCHAR(25),
	Study_midyear FLOAT,
	YearsOfData FLOAT,
	Latitude FLOAT,
	Longitude FLOAT,
	Elevation FLOAT,
	Manipulation TEXT,
	Manipulation_level TEXT,
	Age_ecosystem FLOAT,
	Age_disturbance FLOAT,
	Species TEXT,
	Biome TEXT,
	Ecosystem_type TEXT,
	Ecosystem_state TEXT,
	Leaf_habit TEXT,
	Stage TEXT,
	Soil_type TEXT,
	Soil_drainage TEXT,
	Soil_BD FLOAT,
	Soil_CN FLOAT,
	Soil_sand FLOAT,
	Soil_silt FLOAT,
	Soil_clay FLOAT,
	MAT FLOAT,
	MAP FLOAT,
	PET FLOAT,
	Study_temp FLOAT,
	Study_precip FLOAT,
	Meas_method TEXT,
	Collar_height FLOAT,
	Collar_depth FLOAT,
	Chamber_area FLOAT,
	Time_of_day TEXT,
	Meas_interval FLOAT,
	Annual_coverage FLOAT,
	Partition_method TEXT,
	Rs_annual FLOAT,
	Rs_annual_err FLOAT,
	Rs_interann_err FLOAT,
	Rlitter_annual FLOAT,
	Ra_annual FLOAT,
	Rh_annual FLOAT,
	RC_annual FLOAT,
	Rs_spring FLOAT,
	Rs_summer FLOAT,
	Rs_autumn FLOAT,
	Rs_winter FLOAT,
	Rs_growingseason FLOAT,
	Rs_wet FLOAT,
	Rs_dry FLOAT,
	RC_seasonal FLOAT,
	RC_season TEXT,
	GPP FLOAT,
	ER FLOAT,
	NEP FLOAT,
	NPP FLOAT,
	ANPP FLOAT,
	BNPP FLOAT,
	NPP_FR FLOAT,
	TBCA FLOAT,
	Litter_flux FLOAT,
	Rootlitter_flux FLOAT,
	TotDet_flux FLOAT,
	Ndep FLOAT,
	LAI FLOAT,
	BA FLOAT,
	C_veg_total FLOAT,
	C_AG FLOAT,
	C_BG FLOAT,
	C_CR FLOAT,
	C_FR FLOAT,
	C_litter TEXT,
	C_soilmineral INTEGER,
	C_soildepth INTEGER,
	Notes TEXT
);

-- Now import the data from `srdb-data-V5.csv` into the srdb_data table
SET GLOBAL LOCAL_INFILE=TRUE;
SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';

LOAD DATA LOCAL INFILE '~/Developer/GitHub/SRDBv5-Analytics/data/SRDBv5/srdb-data-V5.csv' -- Change this file path to where you have saved your .csv file
INTO TABLE srdb_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Check whether all rows (10,366) are imported or not
SELECT COUNT(*) FROM srdb_data;


-- Data Cleaning --

-- Before manuplating the data, create a staging or copy of original data
CREATE TABLE stagging_srdb_data
LIKE srdb_data;

INSERT INTO stagging_srdb_data
SELECT * FROM srdb_data;

SELECT * FROM stagging_srdb_data;

-- Unwanted columns can be dropped from the table
ALTER TABLE stagging_srdb_data
DROP COLUMN Author,
DROP COLUMN Duplicate_record,
DROP COLUMN Contributor,
DROP COLUMN Manipulation_level,
DROP COLUMN PET,
DROP COLUMN Collar_height,
DROP COLUMN Collar_depth,
DROP COLUMN Chamber_area,
DROP COLUMN Time_of_day,
DROP COLUMN Partition_method,
DROP COLUMN LAI,
DROP COLUMN Notes;

-- Delete data with Quality_flags: Q10 (potential problem), Q11 (suspected problem), Q12 (known problem), Q13 (duplicate) and Q14 (inconsistency)
DELETE
FROM stagging_srdb_data
WHERE Quality_flag REGEXP 'Q1[0-9]';

-- Remove data without Country name and with Study_midyear, YearsOfData, Latitude and Longitude values as 0
DELETE
FROM stagging_srdb_data
WHERE Country LIKE '';

DELETE
FROM stagging_srdb_data
WHERE Latitude = 0 AND Longitude = 0;

DELETE
FROM stagging_srdb_data
WHERE Latitude = 0 OR Longitude = 0;

DELETE
FROM stagging_srdb_data
WHERE Study_midyear = 0 AND YearsOfData = 0;

DELETE
FROM stagging_srdb_data
WHERE Study_midyear = 0;

-- Unwanted columns are dropped from the table
ALTER TABLE stagging_srdb_data
DROP COLUMN Quality_flag,
DROP COLUMN Soil_BD,
DROP COLUMN Soil_CN,
DROP COLUMN Soil_sand,
DROP COLUMN Soil_silt,
DROP COLUMN Soil_clay,
DROP COLUMN Rs_interann_err,
DROP COLUMN GPP,
DROP COLUMN ER,
DROP COLUMN NEP,
DROP COLUMN NPP,
DROP COLUMN ANPP,
DROP COLUMN BNPP,
DROP COLUMN NPP_FR,
DROP COLUMN Litter_flux,
DROP COLUMN TBCA,
DROP COLUMN Rootlitter_flux,
DROP COLUMN TotDet_flux,
DROP COLUMN Ndep,
DROP COLUMN BA,
DROP COLUMN C_veg_total,
DROP COLUMN C_AG,
DROP COLUMN C_BG,
DROP COLUMN C_CR,
DROP COLUMN C_FR,
DROP COLUMN C_litter,
DROP COLUMN C_soilmineral,
DROP COLUMN C_soildepth;

-- There are some leading and trailing spaces in Ecosystem_type
UPDATE stagging_srdb_data
SET Ecosystem_type = TRIM(Ecosystem_type);

-- Since Record_number is unique set it as primary key
ALTER TABLE stagging_srdb_data
ADD PRIMARY KEY (Record_number);

-- Since there are multiple values in Manipulation and Species columns we can create a new table for them with (Record_number and itself) as PK
-- In Manipulation the delimiter is '; ' and ', ' should standardize
UPDATE stagging_srdb_data
SET Manipulation = REPLACE(Manipulation, ';', ',');

-- Create a temporary table with Record_number and Manipulations
CREATE TEMPORARY TABLE temp_manipulation
WITH first_split AS (
SELECT Record_number,
		IF(
			LOCATE(', ', `Manipulation`) > 0,
			LOWER(SUBSTRING(`Manipulation`, 1, LOCATE(', ', `Manipulation`) - 1)),
			LOWER(`Manipulation`)
		) AS Manipulation_1,
		IF(
			LOCATE(', ', `Manipulation`) > 0,
			LOWER(SUBSTRING(`Manipulation`, LOCATE(', ', `Manipulation`) + 2)),
			NULL
		) AS Manipulation_2
FROM stagging_srdb_data), second_split AS (
SELECT Record_number, Manipulation_1,
		IF(
			LOCATE(', ', `Manipulation_2`) > 0,
			LOWER(SUBSTRING(`Manipulation_2`, 1, LOCATE(', ', `Manipulation_2`) - 1)),
			LOWER(`Manipulation_2`)
		) AS Manipulation_2,
		IF(
			LOCATE(', ', `Manipulation_2`) > 0,
			LOWER(SUBSTRING(`Manipulation_2`, LOCATE(', ', `Manipulation_2`) + 2)),
			NULL
		) AS Manipulation_3
FROM first_split), thrid_split AS (
SELECT Record_number, Manipulation_1, Manipulation_2,
		IF(
			LOCATE(', ', `Manipulation_3`) > 0,
			LOWER(SUBSTRING(`Manipulation_3`, 1, LOCATE(', ', `Manipulation_3`) - 1)),
			LOWER(`Manipulation_3`)
		) AS Manipulation_3,
		IF(
			LOCATE(', ', `Manipulation_3`) > 0,
			LOWER(SUBSTRING(`Manipulation_3`, LOCATE(', ', `Manipulation_3`) + 2)),
			NULL
		) AS Manipulation_4
FROM second_split), fourth_split AS (
SELECT Record_number, Manipulation_1, Manipulation_2, Manipulation_3,
		IF(
			LOCATE(', ', `Manipulation_4`) > 0,
			LOWER(SUBSTRING(`Manipulation_4`, 1, LOCATE(', ', `Manipulation_4`) - 1)),
			LOWER(`Manipulation_4`)
		) AS Manipulation_4,
		IF(
			LOCATE(', ', `Manipulation_4`) > 0,
			LOWER(SUBSTRING(`Manipulation_4`, LOCATE(', ', `Manipulation_4`) + 2)),
			NULL
		) AS Manipulation_5
FROM thrid_split)

SELECT Record_number, Manipulation_1 AS Manipulation
FROM fourth_split
UNION
SELECT Record_number, Manipulation_2
FROM fourth_split
WHERE Manipulation_2 IS NOT NULL
UNION
SELECT Record_number, Manipulation_3
FROM fourth_split
WHERE Manipulation_3 IS NOT NULL
UNION
SELECT Record_number, Manipulation_4
FROM fourth_split
WHERE Manipulation_4 IS NOT NULL
UNION
SELECT Record_number, Manipulation_5
FROM fourth_split
WHERE Manipulation_5 IS NOT NULL;

-- Now the temporary table has all Record_number (repeats if it has multiple Manipulations) and Manipulations
SELECT * FROM temp_manipulation;

-- Create a new table manipulation with Record_number and Manipulations with PK (Record_number, Manipulations) and FK (Record_number)
#DROP TABLE manipulation;
CREATE TABLE manipulation (
	Record_number INTEGER,
    Manipulations VARCHAR(50),
    
    PRIMARY KEY (Record_number, Manipulations),
    FOREIGN KEY (Record_number) REFERENCES stagging_srdb_data(Record_number)
);

-- Insert the data into manipulation from the temporary table
INSERT INTO manipulation
SELECT * FROM temp_manipulation;

-- Trim Manipulations in manipulation
UPDATE manipulation
SET Manipulations = TRIM(Manipulations);

SELECT * FROM manipulation;

-- Now we can drop the temporary table
DROP TEMPORARY TABLE temp_manipulation;

-- In Species the delimeter is not constant it varies from ', ' to '; ' we need to standardize it first
UPDATE stagging_srdb_data
SET Species = REPLACE(Species, ';', ',');

UPDATE stagging_srdb_data
SET Species = REPLACE(Species, 'and ', ', ');

UPDATE stagging_srdb_data
SET Species = REPLACE(Species, ' ,', ', ');

UPDATE stagging_srdb_data
SET Species = REPLACE(Species, ',', ', ');

-- Replace '' values in Species to NULL
UPDATE stagging_srdb_data
SET Species = NULL
WHERE Species LIKE '';

-- Now repeat the process we did for Manipulation
-- Create a temporary table with Record_number and Species
CREATE TEMPORARY TABLE temp_species
WITH first_split AS (
SELECT Record_number,
		IF(
			LOCATE(', ', `Species`) > 0,
			LOWER(SUBSTRING(`Species`, 1, LOCATE(', ', `Species`) - 1)),
			LOWER(`Species`)
		) AS Species_1,
		IF(
			LOCATE(', ', `Species`) > 0,
			LOWER(SUBSTRING(`Species`, LOCATE(', ', `Species`) + 2)),
			NULL
		) AS Species_2
FROM stagging_srdb_data), second_split AS (
SELECT Record_number, Species_1,
		IF(
			LOCATE(', ', `Species_2`) > 0,
			LOWER(SUBSTRING(`Species_2`, 1, LOCATE(', ', `Species_2`) - 1)),
			LOWER(`Species_2`)
		) AS Species_2,
		IF(
			LOCATE(', ', `Species_2`) > 0,
			LOWER(SUBSTRING(`Species_2`, LOCATE(', ', `Species_2`) + 2)),
			NULL
		) AS Species_3
FROM first_split), thrid_split AS (
SELECT Record_number, Species_1, Species_2,
		IF(
			LOCATE(', ', `Species_3`) > 0,
			LOWER(SUBSTRING(`Species_3`, 1, LOCATE(', ', `Species_3`) - 1)),
			LOWER(`Species_3`)
		) AS Species_3,
		IF(
			LOCATE(', ', `Species_3`) > 0,
			LOWER(SUBSTRING(`Species_3`, LOCATE(', ', `Species_3`) + 2)),
			NULL
		) AS Species_4
FROM second_split), fourth_split AS (
SELECT Record_number, Species_1, Species_2, Species_3,
		IF(
			LOCATE(', ', `Species_4`) > 0,
			LOWER(SUBSTRING(`Species_4`, 1, LOCATE(', ', `Species_4`) - 1)),
			LOWER(`Species_4`)
		) AS Species_4,
		IF(
			LOCATE(', ', `Species_4`) > 0,
			LOWER(SUBSTRING(`Species_4`, LOCATE(', ', `Species_4`) + 2)),
			NULL
		) AS Species_5
FROM thrid_split), fifth_split AS (
SELECT Record_number, Species_1, Species_2, Species_3, Species_4,
		IF(
			LOCATE(', ', `Species_5`) > 0,
			LOWER(SUBSTRING(`Species_5`, 1, LOCATE(', ', `Species_5`) - 1)),
			LOWER(`Species_5`)
		) AS Species_5,
		IF(
			LOCATE(', ', `Species_5`) > 0,
			LOWER(SUBSTRING(`Species_5`, LOCATE(', ', `Species_5`) + 2)),
			NULL
		) AS Species_6
FROM fourth_split), sixth_split AS (
SELECT Record_number, Species_1, Species_2, Species_3, Species_4, Species_5,
		IF(
			LOCATE(', ', `Species_6`) > 0,
			LOWER(SUBSTRING(`Species_6`, 1, LOCATE(', ', `Species_6`) - 1)),
			LOWER(`Species_6`)
		) AS Species_6,
		IF(
			LOCATE(', ', `Species_6`) > 0,
			LOWER(SUBSTRING(`Species_6`, LOCATE(', ', `Species_6`) + 2)),
			NULL
		) AS Species_7
FROM fifth_split), seventh_split AS (
SELECT Record_number, Species_1, Species_2, Species_3, Species_4, Species_5, Species_6,
		IF(
			LOCATE(', ', `Species_7`) > 0,
			LOWER(SUBSTRING(`Species_7`, 1, LOCATE(', ', `Species_7`) - 1)),
			LOWER(`Species_7`)
		) AS Species_7,
		IF(
			LOCATE(', ', `Species_7`) > 0,
			LOWER(SUBSTRING(`Species_7`, LOCATE(', ', `Species_7`) + 2)),
			NULL
		) AS Species_8
FROM sixth_split), eigth_split AS (
SELECT Record_number, Species_1, Species_2, Species_3, Species_4, Species_5, Species_6, Species_7,
		IF(
			LOCATE(', ', `Species_8`) > 0,
			LOWER(SUBSTRING(`Species_8`, 1, LOCATE(', ', `Species_8`) - 1)),
			LOWER(`Species_8`)
		) AS Species_8,
		IF(
			LOCATE(', ', `Species_8`) > 0,
			LOWER(SUBSTRING(`Species_8`, LOCATE(', ', `Species_8`) + 2)),
			NULL
		) AS Species_9
FROM seventh_split), ninth_split AS (
SELECT Record_number, Species_1, Species_2, Species_3, Species_4, Species_5, Species_6, Species_7, Species_8,
		IF(
			LOCATE(', ', `Species_9`) > 0,
			LOWER(SUBSTRING(`Species_9`, 1, LOCATE(', ', `Species_9`) - 1)),
			LOWER(`Species_9`)
		) AS Species_9,
		IF(
			LOCATE(', ', `Species_9`) > 0,
			LOWER(SUBSTRING(`Species_9`, LOCATE(', ', `Species_9`) + 2)),
			NULL
		) AS Species_10
FROM eigth_split), tenth_split AS (
SELECT Record_number, Species_1, Species_2, Species_3, Species_4, Species_5, Species_6, Species_7, Species_8, Species_9,
		IF(
			LOCATE(', ', `Species_10`) > 0,
			LOWER(SUBSTRING(`Species_10`, 1, LOCATE(', ', `Species_10`) - 1)),
			LOWER(`Species_10`)
		) AS Species_10,
		IF(
			LOCATE(', ', `Species_10`) > 0,
			LOWER(SUBSTRING(`Species_10`, LOCATE(', ', `Species_10`) + 2)),
			NULL
		) AS Species_11
FROM ninth_split), eleventh_split AS (
SELECT Record_number, Species_1, Species_2, Species_3, Species_4, Species_5, Species_6, Species_7, Species_8, Species_9, Species_10,
		IF(
			LOCATE(', ', `Species_11`) > 0,
			LOWER(SUBSTRING(`Species_11`, 1, LOCATE(', ', `Species_11`) - 1)),
			LOWER(`Species_11`)
		) AS Species_11,
		IF(
			LOCATE(', ', `Species_11`) > 0,
			LOWER(SUBSTRING(`Species_11`, LOCATE(', ', `Species_11`) + 2)),
			NULL
		) AS Species_12
FROM tenth_split)

SELECT Record_number, Species_1 AS Species
FROM eleventh_split
UNION
SELECT Record_number, Species_2
FROM eleventh_split
WHERE Species_2 IS NOT NULL
UNION
SELECT Record_number, Species_3
FROM eleventh_split
WHERE Species_3 IS NOT NULL
UNION
SELECT Record_number, Species_4
FROM eleventh_split
WHERE Species_4 IS NOT NULL
UNION
SELECT Record_number, Species_5
FROM eleventh_split
WHERE Species_5 IS NOT NULL
UNION
SELECT Record_number, Species_6
FROM eleventh_split
WHERE Species_6 IS NOT NULL
UNION
SELECT Record_number, Species_7
FROM eleventh_split
WHERE Species_7 IS NOT NULL
UNION
SELECT Record_number, Species_8
FROM eleventh_split
WHERE Species_8 IS NOT NULL
UNION
SELECT Record_number, Species_9
FROM eleventh_split
WHERE Species_9 IS NOT NULL
UNION
SELECT Record_number, Species_10
FROM eleventh_split
WHERE Species_10 IS NOT NULL
UNION
SELECT Record_number, Species_11
FROM eleventh_split
WHERE Species_11 IS NOT NULL
UNION
SELECT Record_number, Species_12
FROM eleventh_split
WHERE Species_12 IS NOT NULL;

-- Now the temporary table has all Record_number (repeats if it has multiple Species) and Species
SELECT * FROM temp_species;

-- Create a new table species with Record_number and Species with FK (Record_number)
#DROP TABLE species;
CREATE TABLE species (
	Record_number INTEGER,
    Species VARCHAR(50),
    
    FOREIGN KEY (Record_number) REFERENCES stagging_srdb_data(Record_number)
);

-- Insert the data into species from the temporary table
INSERT INTO species
SELECT * FROM temp_species;

-- Trim Species in species and fill '' with NULL
UPDATE species
SET Species = TRIM(Species);

UPDATE species
SET Species = NULL
WHERE Species LIKE '';

SELECT * FROM species;

-- Now we can drop the temporary table
DROP TEMPORARY TABLE temp_species;

-- Now we can drop Species and Manipulation from stagging_srdb_data
ALTER TABLE stagging_srdb_data
DROP COLUMN Species,
DROP COLUMN Manipulation;

-- Fill in the '' values with NULL
UPDATE stagging_srdb_data
SET Ecosystem_state = NULL
WHERE Ecosystem_state LIKE '';

UPDATE stagging_srdb_data
SET Leaf_habit = NULL
WHERE Leaf_habit LIKE '';

UPDATE stagging_srdb_data
SET Stage = NULL
WHERE Stage LIKE '';

-- We are not going to seperate each soil type because the combinations can be specific to the location
UPDATE stagging_srdb_data
SET Soil_type = NULL
WHERE Soil_type LIKE '';

UPDATE stagging_srdb_data
SET Soil_drainage = NULL
WHERE Soil_drainage LIKE '';

SELECT * FROM stagging_srdb_data;

