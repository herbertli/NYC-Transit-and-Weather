-- Create table
CREATE EXTERNAL TABLE fhv_raw (
  pu_t TIMESTAMP,
  do_t TIMESTAMP,
  pu_id INT,
  do_id INT,
  pu_b STRING,
  pu_n STRING,
  do_b STRING,
  do_n STRING,
  pass_n BIGINT,
  prcp DOUBLE,
  snwd DOUBLE,
  snow DOUBLE,
  tavg DOUBLE,
  tmax DOUBLE,
  tmin DOUBLE,
  awnd DOUBLE,
  fog BOOLEAN,
  thunder	BOOLEAN,
  hail BOOLEAN,
  haze BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hl1785/data/fhv/joined/';

-- Create a view
CREATE VIEW fhv AS
  SELECT
    fhv_raw.*,
    hour(fhv_raw.pu_t) AS pu_h,
    minute(fhv_raw.pu_t) AS pu_m,
    dayofmonth(fhv_raw.pu_t) AS pu_d,
    month(fhv_raw.pu_t) AS pu_mon,
    year(fhv_raw.pu_t) AS pu_year
FROM fhv_raw;

DESCRIBE fhv;

-- Show boros and neighborhoods
SELECT DISTINCT pu_b FROM fhv;
SELECT DISTINCT pu_n FROM fhv;

-- # pass when it snows/doesn't
SELECT SUM(pass_n), pu_b
FROM fhv
WHERE snow <> 0 AND pu_year = 2017 AND pu_b <> 'NULL'
GROUP BY pu_b;

SELECT SUM(pass_n), pu_b
FROM fhv
WHERE snow = 0 AND pu_year = 2017 AND pu_b <> 'NULL'
GROUP BY pu_b;

-- # days where it snowed
SELECT COUNT(DISTINCT pu_d, pu_mon, pu_year)
FROM fhv
WHERE snow <> 0 AND pu_year = 2017;

-- # pass when it rains/doesn't
-------------------------------------------------------------
SELECT SUM(pass_n), pu_b
FROM fhv
WHERE prcp <> 0 AND pu_year = 2017 AND pu_b <> 'NULL'
GROUP BY pu_b;

SELECT SUM(pass_n), pu_b
FROM fhv
WHERE prcp = 0 AND pu_year = 2017
GROUP BY pu_b;

SELECT COUNT(DISTINCT pu_d, pu_mon, pu_year)
FROM fhv
WHERE prcp <> 0 AND pu_year = 2017

-- By avg temperature
SELECT SUM(pass_n), AVG(tavg), pu_d, pu_mon, pu_year
FROM fhv
WHERE pu_year = 2017
GROUP BY pu_d, pu_mon, pu_year
ORDER BY pu_mon ASC, pu_d ASC;

SELECT SUM(pass_n)
FROM fhv
WHERE pu_year = 2017 AND (pu_b = "EWR" OR pu_b = "Staten Island")
GROUP BY pu_d, pu_mon, pu_year
ORDER BY pu_mon ASC, pu_d ASC;