-- Create a view
CREATE VIEW green_taxi AS
  SELECT
    greencab.*,
    hour(greencab.pu_t) AS pu_h,
    minute(greencab.pu_t) AS pu_m,
    dayofmonth(greencab.pu_t) AS pu_d,
    month(greencab.pu_t) AS pu_mon,
    year(greencab.pu_t) AS pu_year
FROM greencab;

-- Show table metadata, should look like:
-- +----------+-----------+---------+
-- | name     | type      | comment |
-- +----------+-----------+---------+
-- | pu_t     | timestamp |         |
-- | do_t     | timestamp |         |
-- | distance | double    |         |
-- | pu_id    | int       |         |
-- | do_id    | int       |         |
-- | pu_b     | string    |         |
-- | pu_n     | string    |         |
-- | do_b     | string    |         |
-- | do_n     | string    |         |
-- | pass_n   | bigint    |         |
-- | prcp     | double    |         |
-- | snwd     | double    |         |
-- | snow     | double    |         |
-- | tavg     | double    |         |
-- | tmax     | double    |         |
-- | tmin     | double    |         |
-- | awnd     | double    |         |
-- | fog      | boolean   |         |
-- | thunder  | boolean   |         |
-- | hail     | boolean   |         |
-- | haze     | boolean   |         |
-- | pu_h     | int       |         |
-- | pu_m     | int       |         |
-- | pu_d     | int       |         |
-- | pu_mon   | int       |         |
-- | pu_year  | int       |         |
-- +----------+-----------+---------+
DESCRIBE green_taxi;

-- Show boros and neighborhoods
SELECT DISTINCT pu_b FROM green_taxi;
SELECT DISTINCT pu_n FROM green_taxi;

-- Get hourly usage for January 2017
SELECT SUM(pass_n), pu_h, pu_d, pu_mon, pu_year
FROM green_taxi
WHERE pu_mon = 1 AND pu_year = 2017
GROUP BY pu_h, pu_d, pu_mon, pu_year
ORDER BY pu_d ASC, pu_h ASC;

-- Get usage per borough for January 2017
SELECT SUM(pass_n), pu_h, pu_d, pu_mon, pu_year, pu_b
FROM green_taxi
WHERE pu_mon = 1 AND pu_year = 2017
GROUP BY pu_h, pu_d, pu_mon, pu_year, pu_b
ORDER BY pu_b ASC, pu_d ASC, pu_h ASC;

-- Get hourly usage for April 2017 vs. based on prcp
SELECT SUM(pass_n), pu_d, pu_mon, pu_year, prcp
FROM green_taxi
WHERE pu_mon = 4 AND pu_year = 2017
GROUP BY pu_d, pu_mon, pu_year, prcp
ORDER BY prcp DESC, pu_d ASC;

-- Get daily usage for 2017 vs. avg temp
SELECT SUM(pass_n), pu_d, pu_mon, pu_year, tavg
FROM green_taxi
WHERE pu_year = 2017
GROUP BY pu_d, pu_mon, pu_year, tavg
ORDER BY pu_mon ASC, pu_d ASC;

-- Get hourly usage for Jan 2017 in Queens vs. based on snow
SELECT AVG(pass_n), pu_d, pu_mon, pu_year, snow, pu_b
FROM green_taxi
WHERE pu_year = 2017
GROUP BY pu_d, pu_mon, pu_year, pu_b
ORDER BY snow DESC, pu_d ASC, pu_mon ASC;

-- # pass when it snows/doesn't
SELECT SUM(pass_n), pu_b
FROM green_taxi
WHERE snow <> 0 AND pu_year = 2017
GROUP BY pu_b;

SELECT SUM(pass_n), pu_b
FROM green_taxi
WHERE snow = 0 AND pu_year = 2017
GROUP BY pu_b;

SELECT COUNT(DISTINCT pu_d, pu_mon, pu_year)
FROM green_taxi
WHERE snow <> 0 AND pu_year = 2017

-- # pass when it rains/doesn't
SELECT SUM(pass_n), pu_b
FROM green_taxi
WHERE prcp <> 0
GROUP BY pu_b;

SELECT SUM(pass_n), pu_b
FROM green_taxi
WHERE prcp = 0
GROUP BY pu_b;

SELECT COUNT(DISTINCT pu_d, pu_mon, pu_year)
FROM green_taxi
WHERE prcp <> 0 AND pu_year = 2017

-- By avg temperature
SELECT SUM(pass_n), AVG(tavg), pu_d, pu_mon, pu_year
FROM green_taxi
WHERE pu_year = 2017
GROUP BY pu_d, pu_mon, pu_year
ORDER BY pu_mon ASC, pu_d ASC;

SELECT SUM(pass_n)
FROM green_taxi
WHERE pu_year = 2017 AND (pu_b = "EWR" OR pu_b = "Staten Island")
GROUP BY pu_d, pu_mon, pu_year
ORDER BY pu_mon ASC, pu_d ASC;