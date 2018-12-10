-- Create a table
CREATE EXTERNAL TABLE yellowtaxi (
  pu_t TIMESTAMP,
  do_t TIMESTAMP,
  distance DOUBLE,
  pu_b STRING,
  pu_n STRING,
  do_b STRING,
  do_n STRING,
  pass_n BIGINT,
  prcp DOUBLE,
  prcp_b INT,
  snwd DOUBLE,
  snow DOUBLE,
  snow_b INT,
  tavg DOUBLE,
  tmax DOUBLE,
  tmin DOUBLE,
  awnd DOUBLE,
  fog STRING,
  thunder	STRING,
  hail STRING,
  haze STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hl1785/data/yellow/joined/';

-- Create a view
CREATE VIEW yellow_taxi AS
  SELECT
    yellowtaxi.*,
    hour(yellowtaxi.pu_t) AS pu_h,
    minute(yellowtaxi.pu_t) AS pu_m,
    dayofmonth(yellowtaxi.pu_t) AS pu_d,
    month(yellowtaxi.pu_t) AS pu_mon,
    year(yellowtaxi.pu_t) AS pu_year
FROM yellowtaxi;

DESCRIBE yellow_taxi;

-- Show boros and neighborhoods
SELECT DISTINCT pu_b FROM yellow_taxi;
SELECT DISTINCT pu_n FROM yellow_taxi;

-- # pass when it snows/doesn't
SELECT SUM(pass_n), pu_b
FROM yellow_taxi
WHERE snow <> 0 AND pu_year = 2017
GROUP BY pu_b;

SELECT SUM(pass_n), pu_b
FROM yellow_taxi
WHERE snow = 0 AND pu_year = 2017
GROUP BY pu_b;

-- # days where it snowed
SELECT COUNT(DISTINCT pu_d, pu_mon, pu_year)
FROM yellow_taxi
WHERE snow <> 0 AND pu_year = 2017

-- # pass when it rains/doesn't
-------------------------------------------------------------
SELECT SUM(pass_n), pu_b
FROM yellow_taxi
WHERE prcp <> 0 AND pu_year = 2017
GROUP BY pu_b;

SELECT SUM(pass_n), pu_b
FROM yellow_taxi
WHERE prcp = 0 AND pu_year = 2017
GROUP BY pu_b;

SELECT COUNT(DISTINCT pu_d, pu_mon, pu_year)
FROM yellow_taxi
WHERE prcp <> 0 AND pu_year = 2017

-- By avg temperature
SELECT SUM(pass_n), AVG(tavg), pu_d, pu_mon, pu_year
FROM yellow_taxi
WHERE pu_year = 2017
GROUP BY pu_d, pu_mon, pu_year
ORDER BY pu_mon ASC, pu_d ASC;

SELECT SUM(pass_n)
FROM yellow_taxi
WHERE pu_year = 2017 AND (pu_b = "EWR" OR pu_b = "Staten Island")
GROUP BY pu_d, pu_mon, pu_year
ORDER BY pu_mon ASC, pu_d ASC;