USE SRDBv5;

SELECT * FROM stagging_srdb_data;

-- Number of samples from each country
SELECT Country, COUNT(*) AS cnt
FROM stagging_srdb_data
GROUP BY Country
ORDER BY cnt DESC;

SELECT Biome, COUNT(*) 
FROM stagging_srdb_data
GROUP BY Biome
ORDER BY Biome;

SELECT Ecosystem_type, COUNT(*) 
FROM stagging_srdb_data
GROUP BY Ecosystem_type
ORDER BY Ecosystem_type;

SELECT Ecosystem_state, COUNT(*) 
FROM stagging_srdb_data
GROUP BY Ecosystem_state
ORDER BY Ecosystem_state;

SELECT Leaf_habit, COUNT(*) 
FROM stagging_srdb_data
GROUP BY Leaf_habit
ORDER BY Leaf_habit;

SELECT Stage, COUNT(*) 
FROM stagging_srdb_data
GROUP BY Stage
ORDER BY Stage;

SELECT Soil_type, COUNT(*) 
FROM stagging_srdb_data
GROUP BY Soil_type
ORDER BY Soil_type;

SELECT Soil_drainage, COUNT(*) 
FROM stagging_srdb_data
GROUP BY Soil_drainage
ORDER BY Soil_drainage;

SELECT Manipulations, COUNT(*) 
FROM manipulation
GROUP BY Manipulations
ORDER BY Manipulations;

SELECT Species, COUNT(*) 
FROM species
GROUP BY Species
ORDER BY Species;

-- There are a total of 1910 species types mentioned in this data set
WITH species_type AS (
SELECT Species, COUNT(*) 
FROM species
GROUP BY Species
ORDER BY Species)
SELECT COUNT(*)
FROM species_type
WHERE Species IS NOT NULL;

-- Create a temporary table or a view for stagging_srdb_data + manipulation
CREATE OR REPLACE VIEW srdb_data_manipulation AS
SELECT *
FROM stagging_srdb_data AS srdb
NATURAL JOIN manipulation AS mnp;

SELECT * FROM srdb_data_manipulation;

-- Create a temporary table or a view for stagging_srdb_data + species 
-- (temporary tables are faster that views but they reside in RAM so it makes the computer a bit slow)
CREATE OR REPLACE VIEW srdb_data_species AS
SELECT *
FROM stagging_srdb_data AS srdb
NATURAL JOIN species AS sps;

SELECT * FROM srdb_data_species;

-- Top 10 species in the entire dataset
SELECT Species, COUNT(*) AS cnt
FROM species
GROUP BY Species
HAVING Species IS NOT NULL AND Species != 'none'
ORDER BY cnt DESC
LIMIT 10;

-- Countries that grow the top 10 species
WITH top_species AS (
SELECT Species, COUNT(*) AS cnt
FROM species
GROUP BY Species
HAVING Species IS NOT NULL AND Species != 'none'
ORDER BY cnt DESC
LIMIT 10)

SELECT Country, Species, COUNT(*) AS num_of_records
FROM srdb_data_species
GROUP BY Country, Species
HAVING Species IN (SELECT Species FROM top_species)
ORDER BY num_of_records DESC;

-- Ranking top 3 (Country (based on # of records), Study_midyear) using average Rs_annual
WITH top_countries AS (
SELECT Country, COUNT(*) AS cnt
FROM stagging_srdb_data
GROUP BY Country
ORDER BY cnt DESC
LIMIT 3),
country_year_rsannual AS (
SELECT Country, Study_midyear, AVG(Rs_annual) AS avg_Rs_annual, COUNT(*) AS num_of_records
FROM stagging_srdb_data
WHERE Rs_annual > 0
GROUP BY Country, Study_midyear
HAVING Country IN (SELECT Country FROM top_countries)),
country_year_rsannual_ranking AS (
SELECT *, DENSE_RANK() OVER(PARTITION BY Country ORDER BY avg_Rs_annual DESC) AS ranking
FROM country_year_rsannual)

SELECT * 
FROM country_year_rsannual_ranking
WHERE ranking <= 5;

-- Ranking top 5 ((Country, Regions) (based on # of records), FLOOR(Study_midyear))  using average Rs_annual
WITH top_country_region AS (
SELECT Country, Region, COUNT(*) AS cnt
FROM stagging_srdb_data
GROUP BY Country, Region
HAVING Region != ''
ORDER BY cnt DESC
LIMIT 5),
country_region_year_rsannual AS (
SELECT Country, Region, FLOOR(Study_midyear) AS Study_year, AVG(Rs_annual) AS avg_Rs_annual, COUNT(*) AS num_of_records
FROM stagging_srdb_data
WHERE Rs_annual > 0
GROUP BY Country, Region, FLOOR(Study_midyear)
HAVING Country IN (SELECT Country FROM top_country_region) AND Region IN (SELECT Region FROM top_country_region)),
country_region_year_rsannual_ranking AS (
SELECT *, DENSE_RANK() OVER(PARTITION BY Country ORDER BY avg_Rs_annual DESC) AS ranking
FROM country_region_year_rsannual)

SELECT * 
FROM country_region_year_rsannual_ranking
WHERE ranking <= 5;


SELECT Country, AVG(Rs_annual) AS avg_Rs_annual
FROM stagging_srdb_data
WHERE Rs_annual > 0
GROUP BY Country
ORDER BY avg_Rs_annual DESC;


