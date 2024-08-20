-- Create a new SCHEMA named SRDBv5
#DROP SCHEMA `SRDBv5`;
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


-- Create srdb_eqns table with column names and data types from `srdb-equations_fields_V5.txt`
#DROP TABLE srdb_eqns;
CREATE TABLE srdb_eqns (
	Record_number INTEGER,
	Model_type TEXT,
	Temp_effect VARCHAR(10),
	Model_output_units TEXT,
	Model_temp_min INTEGER,
	Model_temp_max INTEGER,
	Model_N DOUBLE,
	Model_R2 DOUBLE,
	T_depth DOUBLE,
	Model_paramA DOUBLE,
	Model_paramB DOUBLE,
	Model_paramC DOUBLE,
	Model_paramD DOUBLE,
	Model_paramE DOUBLE,
	WC_effect VARCHAR(10),
	R10 DOUBLE,
	Q10_0_10 DOUBLE,
	Q10_5_15 DOUBLE,
	Q10_10_20 DOUBLE,
	Q10_0_20 DOUBLE,
	Q10_other1 DOUBLE,
	Q10_other1_temp_min DOUBLE,
	Q10_other1_temp_max DOUBLE,
	Q10_other2 DOUBLE,
	Q10_other2_temp_min DOUBLE,
	Q10_other2_temp_max DOUBLE
);

-- Now import the data from `srdb-equations-V5.csv` into the srdb_eqns table
LOAD DATA LOCAL INFILE '~/Developer/GitHub/SRDBv5-Analytics/data/SRDBv5/srdb-equations-V5.csv' -- Change this file path to where you have saved your .csv file
INTO TABLE srdb_eqns
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Check whether all rows (3,318) are imported or not
SELECT COUNT(*) FROM srdb_eqns;


