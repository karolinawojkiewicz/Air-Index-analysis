/*

Hi, this is my first data analysis project. This particular project demonstrates data cleaning and exploration with MySql. 
The cleaned data was exported to MS Excel for visualization purposes. 
The dataset contains the Air Index for cities from 133 countries since 2017 to 2022. 
The year 2022 is further divided into months.
Dataset used in project comes from Kaggle: https://www.kaggle.com/datasets/bilalwaseer/air-index-of-worlds-all-cities-2017-to-2022.
Dataset was downloaded on 11/08/2023.

*/

-- Creating database
CREATE DATABASE project_air_index;
USE project_air_index;

-- Creating table for data. Varchar datatype have been choose in purpose as there were multiple errors during importing data to columns with other datatypes.
CREATE TABLE air_index_data 
(ID INT,
Country VARCHAR (100),
City VARCHAR (100),
`2022` VARCHAR (100),
JAN VARCHAR (100),
FEB VARCHAR (100),
MAR VARCHAR (100),
APR VARCHAR (100),
MAY VARCHAR (100),
JUN VARCHAR (100),
JUL VARCHAR (100),
AUG VARCHAR (100),
SEP VARCHAR (100),
OCT VARCHAR (100),
NOV VARCHAR (100),
`DEC` VARCHAR (100),
`2021` VARCHAR (100),
`2020` VARCHAR (100),
`2019` VARCHAR (100),
`2018` VARCHAR (100),
`2017` VARCHAR (100));

-- the data has been uploaded via data import wizard
SELECT * FROM air_index_data;

-- missing data are set as ''. I have changed that to null value
UPDATE air_index_data
SET `2022` = null
where `2022` = '' ;

UPDATE air_index_data
SET JAN = null
where JAN = '' ;

UPDATE air_index_data
SET FEB = null
where FEB = '' ;

UPDATE air_index_data
SET MAR = null
where MAR = ''; 
  
UPDATE air_index_data
SET APR = null
where APR = '' ;

UPDATE air_index_data
SET MAY = null
where MAY = '' ;

UPDATE air_index_data
SET JUN = null
where JUN = '' ;

UPDATE air_index_data
SET JUL = null
where JUL = '' ;
   
UPDATE air_index_data
SET AUG = null
where AUG = '' ;

UPDATE air_index_data
SET SEP = null
where SEP = '' ;

UPDATE air_index_data
SET OCT = null
where OCT = '' ;

UPDATE air_index_data
SET NOV = null
where NOV = '' ;
   
UPDATE air_index_data
SET `DEC` = null
where `DEC` = '' ;
 
UPDATE air_index_data
SET `2021` = null
where `2021` = '' ;

UPDATE air_index_data
SET `2019` = null
where `2019` = '' ;

UPDATE air_index_data
SET `2018` = null
where `2018` = '' ;

UPDATE air_index_data
SET `2017` = null
where `2017` = '' ;

UPDATE air_index_data
SET `2020` = null
where `2020` = '' ;

-- Changing datatype for colunns contains numbers
ALTER TABLE air_index_data MODIFY `2022` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `JAN` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `FEB` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `MAR` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `APR` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `MAY` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `JUN` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `JUL` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `AUG` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `SEP` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `OCT` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `NOV` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `DEC` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `2021` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `2020` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `2019` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `2018` DECIMAL (10,2);
ALTER TABLE air_index_data MODIFY `2017` DECIMAL (10,2);

-- missing country in record no 812, online checked completed Wiang Phang Kham is located in Thailand. Updating the view
SELECT * FROM air_index_data
WHERE Country = '';

 UPDATE air_index_data
 SET Country = 'Thailand'
 where Country = '';
 
 SELECT * From air_index_data;

-- Checking and showing duplicates
WITH cte AS 
	(SELECT *, ROW_NUMBER () OVER (PARTITION BY City, Country ORDER BY City) as number_of_records_for_particular_city 
	FROM air_index_data)
    SELECT DISTINCT a.ID, a.Country, a.City, a.`2022`, a.JAN, a.FEB, a.MAR, a.APR, a.MAY, a.JUN ,a.JUL, a.AUG, a.SEP, a.OCT, a.NOV, a.`DEC`,
				a.`2021`, a.`2020`, a.`2019`, a.`2018`, a.`2017`, a.number_of_records_for_particular_city 
    FROM cte a
    CROSS JOIN cte b on a.Country = b.Country AND a.City = b.City 
    AND a.number_of_records_for_particular_city <> b.number_of_records_for_particular_city;
    
 -- When the city and country is the same and values of air index are different for particular columns I set the avarge value for those cells.
 -- When one value is null I set a value from row which contains value. This is the reason why I do not change null to zero when I calculate avg
 
	SELECT Country, City, 
    AVG(`2022`) AS avg_2022, 
	AVG(JAN) AS avg_jan, 
    AVG(FEB) AS avg_feb, 
    AVG(MAR) AS avg_mar, 
	AVG(APR) AS avg_apr, 
    AVG(MAY) AS avg_may, 
    AVG(JUN) AS avg_jun, 
	AVG(JUL) AS avg_jul, 
    AVG(AUG) AS avg_aug, 
    AVG(SEP) AS avg_sep,
	AVG(OCT) AS avg_oct, 
    AVG(NOV) AS avg_nov, 
	AVG(`DEC`) AS avg_dec, 
    AVG(`2021`) AS avg_2021,
	AVG(`2020`) AS avg_2020, 
    AVG(`2019`) AS avg_2019,
	AVG(`2018`) AS avg_2018, 
    AVG(`2017`) AS avg_2017
	FROM
    (WITH cte AS 
	(SELECT *, ROW_NUMBER () OVER (PARTITION BY City, Country ORDER BY City) as number_of_records_for_particular_city 
	FROM air_index_data)
    SELECT DISTINCT a.ID, a.Country, a.City, a.`2022`, a.JAN, a.FEB, a.MAR, a.APR, a.MAY, a.JUN ,a.JUL, a.AUG, a.SEP, a.OCT, a.NOV, a.`DEC`,
				a.`2021`, a.`2020`, a.`2019`, a.`2018`, a.`2017`, a.number_of_records_for_particular_city 
    FROM cte a
    CROSS JOIN cte b on a.Country = b.Country AND a.City = b.City 
    AND a.number_of_records_for_particular_city <> b.number_of_records_for_particular_city) a
    GROUP BY 1,2;
    
    -- Checking if values for particular colums are the same as values calculated above. If yes, the primary values is set, if not the avg 
    -- (only for duplicates). I needed to change null values to 0 for correct use of <>. 
    -- I used RIGHT JOIN to show all cleaned values. 
    
	WITH cte1 AS
	(SELECT Country, City, 
    AVG(`2022`) AS avg_2022, 
	AVG(JAN) AS avg_jan, 
    AVG(FEB) AS avg_feb, 
    AVG(MAR) AS avg_mar, 
	AVG(APR) AS avg_apr, 
    AVG(MAY) AS avg_may, 
    AVG(JUN) AS avg_jun, 
	AVG(JUL) AS avg_jul, 
    AVG(AUG) AS avg_aug, 
    AVG(SEP) AS avg_sep,
	AVG(OCT) AS avg_oct, 
    AVG(NOV) AS avg_nov, 
	AVG(`DEC`) AS avg_dec, 
    AVG(`2021`) AS avg_2021,
	AVG(`2020`) AS avg_2020, 
    AVG(`2019`) AS avg_2019,
	AVG(`2018`) AS avg_2018, 
    AVG(`2017`) AS avg_2017
	FROM
		(WITH cte AS 
		(SELECT *, ROW_NUMBER () OVER (PARTITION BY City, Country ORDER BY City) as number_of_records_for_particular_city 
		FROM air_index_data)
		SELECT DISTINCT a.ID, a.Country, a.City, a.`2022`, a.JAN, a.FEB, a.MAR, a.APR, a.MAY, a.JUN ,a.JUL, a.AUG, a.SEP, a.OCT, a.NOV, a.`DEC`,
				a.`2021`, a.`2020`, a.`2019`, a.`2018`, a.`2017`, a.number_of_records_for_particular_city 
		FROM cte a
    CROSS JOIN cte b on a.Country = b.Country AND a.City = b.City 
    AND a.number_of_records_for_particular_city <> b.number_of_records_for_particular_city) a
    GROUP BY 1,2) 
SELECT b.ID, b.Country, b.City,
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2022`,0) <> avg_2022 THEN avg_2022
	ELSE b.`2022`
	END AS `2022`,
	
    CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`JAN`,0) <> avg_JAN THEN avg_JAN
	ELSE b.`JAN`
	END AS `JAN`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`FEB`,0) <> avg_FEB THEN avg_FEB
	ELSE b.`FEB`
	END AS `FEB`,
	
    CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`MAR`,0) <> avg_MAR THEN avg_MAR
	ELSE b.`MAR`
	END AS `MAR`,
	
    CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`APR`,0) <> avg_APR THEN avg_APR
	ELSE b.`APR`
	END AS `APR`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`MAY`,0) <> avg_MAY THEN avg_MAY
	ELSE b.`MAY`
	END AS `MAY`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`JUN`,0) <> avg_JUN THEN avg_JUN
	ELSE b.`JUN`
	END AS `JUN`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`JUL`,0) <> avg_JUL THEN avg_JUL
	ELSE b.`JUL`
	END AS `JUL`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`AUG`,0) <> avg_AUG THEN avg_AUG
	ELSE b.`AUG`
	END AS `AUG`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`SEP`,0) <> avg_SEP THEN avg_SEP
	ELSE b.`SEP`
	END AS `SEP`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`OCT`,0) <> avg_OCT THEN avg_OCT
	ELSE b.`OCT`
	END AS `OCT`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`NOV`,0) <> avg_NOV THEN avg_NOV
	ELSE b.`NOV`
	END AS `NOV`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`DEC`,0) <> avg_DEC THEN avg_DEC
	ELSE b.`DEC`
	END AS `DEC`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2021`,0) <> avg_2021 THEN avg_2021
	ELSE b.`2021`
	END AS `2021`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2020`,0) <> avg_2020 THEN avg_2020
	ELSE b.`2020`
	END AS `2020`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2019`,0) <> avg_2019 THEN avg_2019
	ELSE b.`2019`
	END AS `2019`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2018`,0) <> avg_2018 THEN avg_2018
	ELSE b.`2018`
	END AS `2018`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2017`, 0) <> avg_2017 THEN avg_2017
	ELSE b.`2017`
	END AS `2017`
FROM cte1 a
RIGHT JOIN air_index_data b ON a.City = b.City AND a.Country = b.Country
ORDER BY b.City, b.Country;
DROP VIEW cleaned_data_air_index;

CREATE VIEW cleaned_data_air_index AS (
WITH cte2 AS(
WITH cte1 AS
	(SELECT Country, City, 
    AVG(`2022`) AS avg_2022, 
	AVG(JAN) AS avg_jan, 
    AVG(FEB) AS avg_feb, 
    AVG(MAR) AS avg_mar, 
	AVG(APR) AS avg_apr, 
    AVG(MAY) AS avg_may, 
    AVG(JUN) AS avg_jun, 
	AVG(JUL) AS avg_jul, 
    AVG(AUG) AS avg_aug, 
    AVG(SEP) AS avg_sep,
	AVG(OCT) AS avg_oct, 
    AVG(NOV) AS avg_nov, 
	AVG(`DEC`) AS avg_dec, 
    AVG(`2021`) AS avg_2021,
	AVG(`2020`) AS avg_2020, 
    AVG(`2019`) AS avg_2019,
	AVG(`2018`) AS avg_2018, 
    AVG(`2017`) AS avg_2017
	FROM
		(WITH cte AS 
		(SELECT *, ROW_NUMBER () OVER (PARTITION BY City, Country ORDER BY City) as number_of_records_for_particular_city 
		FROM air_index_data)
		SELECT DISTINCT a.ID, a.Country, a.City, a.`2022`, a.JAN, a.FEB, a.MAR, a.APR, a.MAY, a.JUN ,a.JUL, a.AUG, a.SEP, a.OCT, a.NOV, a.`DEC`,
				a.`2021`, a.`2020`, a.`2019`, a.`2018`, a.`2017`, a.number_of_records_for_particular_city 
		FROM cte a
    CROSS JOIN cte b on a.Country = b.Country AND a.City = b.City 
    AND a.number_of_records_for_particular_city <> b.number_of_records_for_particular_city) a
    GROUP BY 1,2) 
SELECT b.ID, b.Country, b.City,
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2022`,0) <> avg_2022 THEN avg_2022
	ELSE b.`2022`
	END AS `2022`,
	
    CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`JAN`,0) <> avg_JAN THEN avg_JAN
	ELSE b.`JAN`
	END AS `JAN`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`FEB`,0) <> avg_FEB THEN avg_FEB
	ELSE b.`FEB`
	END AS `FEB`,
	
    CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`MAR`,0) <> avg_MAR THEN avg_MAR
	ELSE b.`MAR`
	END AS `MAR`,
	
    CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`APR`,0) <> avg_APR THEN avg_APR
	ELSE b.`APR`
	END AS `APR`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`MAY`,0) <> avg_MAY THEN avg_MAY
	ELSE b.`MAY`
	END AS `MAY`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`JUN`,0) <> avg_JUN THEN avg_JUN
	ELSE b.`JUN`
	END AS `JUN`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`JUL`,0) <> avg_JUL THEN avg_JUL
	ELSE b.`JUL`
	END AS `JUL`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`AUG`,0) <> avg_AUG THEN avg_AUG
	ELSE b.`AUG`
	END AS `AUG`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`SEP`,0) <> avg_SEP THEN avg_SEP
	ELSE b.`SEP`
	END AS `SEP`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`OCT`,0) <> avg_OCT THEN avg_OCT
	ELSE b.`OCT`
	END AS `OCT`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`NOV`,0) <> avg_NOV THEN avg_NOV
	ELSE b.`NOV`
	END AS `NOV`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`DEC`,0) <> avg_DEC THEN avg_DEC
	ELSE b.`DEC`
	END AS `DEC`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2021`,0) <> avg_2021 THEN avg_2021
	ELSE b.`2021`
	END AS `2021`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2020`,0) <> avg_2020 THEN avg_2020
	ELSE b.`2020`
	END AS `2020`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2019`,0) <> avg_2019 THEN avg_2019
	ELSE b.`2019`
	END AS `2019`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2018`,0) <> avg_2018 THEN avg_2018
	ELSE b.`2018`
	END AS `2018`,
    
	CASE 
	WHEN b.Country = a.Country AND b.City = a.City AND IFNULL(b.`2017`, 0) <> avg_2017 THEN avg_2017
	ELSE b.`2017`
	END AS `2017`,
    ROW_NUMBER () OVER (PARTITION BY City, Country ORDER BY City) as numbers
FROM cte1 a
RIGHT JOIN air_index_data b ON a.City = b.City AND a.Country = b.Country
ORDER BY b.City, b.Country
)
SELECT ID, Country, City, 
ROUND(`2022`,2) AS `2022`, 
ROUND(JAN,2) AS JAN, 
ROUND(FEB,2) AS FEB, 
ROUND(MAR,2) AS MAR, 
ROUND(APR,2) AS APR, 
ROUND(MAY,2) AS MAY, 
ROUND(JUN,2) AS JUN,
ROUND(JUL,2) AS JUL,
ROUND(AUG,2) AS AUG, 
ROUND(SEP,2) AS SEP, 
ROUND(OCT,2) AS OCT, 
ROUND(NOV,2) AS NOV, 
ROUND(`DEC`,2) AS `DEC`, 
ROUND(`2021`,2) AS `2021`,
ROUND(`2020`,2) AS `2020`,
ROUND(`2019`,2) AS `2019`,
ROUND(`2018`,2) AS `2018`, 
ROUND(`2017`,2) AS `2017`
FROM cte2
WHERE numbers = 1
ORDER BY ID);

-- Showing cleaned data
SELECT * FROM cleaned_data_air_index;



-- Exploring data with SQL

-- 1) Which country has the most polluted air in each year (top 10 countries)?

-- in 2022

SELECT Country, ROUND(SUM(COALESCE(`2022`, 0)),2) AS sum_2022 
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- in 2021

SELECT Country, ROUND(SUM(COALESCE(`2021`, 0)),2) AS sum_2021
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- in 2020

SELECT Country, ROUND(SUM(COALESCE(`2020`, 0)),2) AS sum_2020
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- in 2019

SELECT Country, ROUND(SUM(COALESCE(`2019`, 0)),2) AS sum_2019
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- in 2018

SELECT Country, ROUND(SUM(COALESCE(`2018`, 0)),2) AS sum_2018
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- in 2017

SELECT Country, ROUND(SUM(COALESCE(`2017`, 0)),2) AS sum_2017
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 2) What country has the least polluted air in each year (top 10 countries)?

-- in 2022

SELECT Country, ROUND(SUM(COALESCE(`2022`, 0)),2) AS sum_2022 
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 
LIMIT 10;

-- in 2021

SELECT Country, ROUND(SUM(COALESCE(`2021`, 0)),2) AS sum_2021
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 
LIMIT 10;

-- in 2020

SELECT Country, ROUND(SUM(COALESCE(`2020`, 0)),2) AS sum_2020
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 
LIMIT 10;

-- in 2019

SELECT Country, ROUND(SUM(COALESCE(`2019`, 0)),2) AS sum_2019
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 
LIMIT 10;

-- in 2018

SELECT Country, ROUND(SUM(COALESCE(`2018`, 0)),2) AS sum_2018
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 
LIMIT 10;

-- in 2017

SELECT Country, ROUND(SUM(COALESCE(`2017`, 0)),2) AS sum_2017
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY 2 
LIMIT 10;

-- 3) Which country has the biggest air polution within 6 years (top 10)
SELECT Country, 
IFNULL(SUM(`2022`),0) + IFNULL(SUM(`2021`),0) + IFNULL(SUM(`2020`),0) + 
IFNULL(SUM(`2019`),0) + IFNULL(SUM(`2018`),0) + IFNULL(SUM(`2017`),0) AS total_air_index 
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY total_air_index  DESC
LIMIT 10;

-- 4) Which City has the biggest air polution within 6 years (top 10)
SELECT Country, City,
IFNULL(SUM(`2022`),0) + IFNULL(SUM(`2021`),0) + IFNULL(SUM(`2020`),0) + 
IFNULL(SUM(`2019`),0) + IFNULL(SUM(`2018`),0) + IFNULL(SUM(`2017`),0) AS total_air_index 
FROM cleaned_data_air_index
GROUP BY 2,1
ORDER BY total_air_index  DESC
LIMIT 10;

-- 5) Which country has the least air polution within 6 years (top 10)
SELECT Country, 
IFNULL(SUM(`2022`),0) + IFNULL(SUM(`2021`),0) + IFNULL(SUM(`2020`),0) + 
IFNULL(SUM(`2019`),0) + IFNULL(SUM(`2018`),0) + IFNULL(SUM(`2017`),0) AS total_air_index 
FROM cleaned_data_air_index
GROUP BY 1
ORDER BY total_air_index 
LIMIT 10;

-- 6) Which City has the biggest air polution within 6 years (top 10)
SELECT Country, City,
IFNULL(SUM(`2022`),0) + IFNULL(SUM(`2021`),0) + IFNULL(SUM(`2020`),0) + 
IFNULL(SUM(`2019`),0) + IFNULL(SUM(`2018`),0) + IFNULL(SUM(`2017`),0) AS total_air_index 
FROM cleaned_data_air_index
GROUP BY 2,1
ORDER BY total_air_index 
LIMIT 10;

-- 7) Percentage for each year grouping by country

SELECT Country, ROUND(IFNULL(SUM(`2022`),0)/(SELECT IFNULL(SUM(`2022`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2022,
ROUND(IFNULL(SUM(`2021`),0)/(SELECT IFNULL(SUM(`2021`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2021,
ROUND(IFNULL(SUM(`2020`),0)/(SELECT IFNULL(SUM(`2020`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2020,
ROUND(IFNULL(SUM(`2019`),0)/(SELECT IFNULL(SUM(`2019`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2019,
ROUND(IFNULL(SUM(`2018`),0)/(SELECT IFNULL(SUM(`2018`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2018,
ROUND(IFNULL(SUM(`2017`),0)/(SELECT IFNULL(SUM(`2017`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2017
FROM cleaned_data_air_index
GROUP BY 1;

-- 8) Percentage for each year grouping by city

SELECT Country, City, ROUND(IFNULL(SUM(`2022`),0)/(SELECT IFNULL(SUM(`2022`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2022,
ROUND(IFNULL(SUM(`2021`),0)/(SELECT IFNULL(SUM(`2021`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2021,
ROUND(IFNULL(SUM(`2020`),0)/(SELECT IFNULL(SUM(`2020`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2020,
ROUND(IFNULL(SUM(`2019`),0)/(SELECT IFNULL(SUM(`2019`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2019,
ROUND(IFNULL(SUM(`2018`),0)/(SELECT IFNULL(SUM(`2018`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2018,
ROUND(IFNULL(SUM(`2017`),0)/(SELECT IFNULL(SUM(`2017`),0) FROM cleaned_data_air_index)*100,2) AS percentage_2017
FROM cleaned_data_air_index
GROUP BY 2,1
LIMIT 5;

-- 9) TOP 10 Countries which air quality has been changed to worse/better

	WITH cte as (
	SELECT Country, 
    CASE WHEN IFNULL(`2021`, 0) - IFNULL(`2022`,0) > 0 THEN 'better'
	ELSE 'worse'
	END AS difference_2021_2022,
    
	CASE WHEN IFNULL(`2020`, 0) - IFNULL(`2021`,0) > 0 THEN 'better'
	ELSE 'worse'
	END AS difference_2020_2021,

	CASE WHEN IFNULL(`2019`, 0) - IFNULL(`2020`,0) > 0 THEN 'better'
	ELSE 'worse'
	END AS difference_2019_2020,

	CASE WHEN IFNULL(`2018`, 0) - IFNULL(`2019`,0) > 0 THEN 'better'
	ELSE 'worse'
	END AS difference_2018_2019,

	CASE WHEN IFNULL(`2017`, 0) - IFNULL(`2018`,0) > 0 THEN 'better'
	ELSE 'worse'
	END AS difference_2017_2018

	FROM
    (SELECT Country, IFNULL(SUM(`2022`),0) AS `2022`,
    IFNULL(SUM(`2021`),0) AS `2021`,
	IFNULL(SUM(`2020`),0) AS `2020`, 
    IFNULL(SUM(`2019`),0) AS `2019`,
	IFNULL(SUM(`2018`),0) AS `2018`,
    IFNULL(SUM(`2017`),0) AS `2017` 
    FROM cleaned_data_air_index
	GROUP BY 1) a)
    SELECT * FROM cte;

-- 10) Percentage of air polution for each city in their country

WITH cte AS (
SELECT *, SUM(`2022`) OVER (PARTITION BY Country) AS Sum_for_each_country_in_2022,
SUM(`2021`) OVER (PARTITION BY Country) AS Sum_for_each_country_in_2021,
SUM(`2020`) OVER (PARTITION BY Country) AS Sum_for_each_country_in_2020,
SUM(`2019`) OVER (PARTITION BY Country) AS Sum_for_each_country_in_2019,
SUM(`2018`) OVER (PARTITION BY Country) AS Sum_for_each_country_in_2018,
SUM(`2017`) OVER (PARTITION BY Country) AS Sum_for_each_country_in_2017
FROM cleaned_data_air_index)
SELECT Country, City, 
ROUND((`2022`/Sum_for_each_country_in_2022 *100),2) AS percentage_for_each_city_in_2022,
ROUND((`2021`/Sum_for_each_country_in_2021 *100),2) AS percentage_for_each_city_in_2021,
ROUND((`2020`/Sum_for_each_country_in_2020 *100),2) AS percentage_for_each_city_in_2020,
ROUND((`2019`/Sum_for_each_country_in_2019 *100),2) AS percentage_for_each_city_in_2019,
ROUND((`2018`/Sum_for_each_country_in_2018 *100),2) AS percentage_for_each_city_in_2018,
ROUND((`2017`/Sum_for_each_country_in_2017 *100),2) AS percentage_for_each_city_in_2017
FROM cte
ORDER BY Country;

-- 11) Avarage Air Index for Poland from 2017 to 2022

SELECT Country,
AVG(`2021`) as avg_2021,
AVG(`2020`) as avg_2020,
AVG(`2019`) as avg_2019,
AVG(`2018`) as avg_2018,
AVG(`2017`) as avg_2017,
COUNT(City) as no_of_city
FROM cleaned_data_air_index
WHERE Country ="Poland"
Group by 1;
---
