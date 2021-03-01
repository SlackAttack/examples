/*===============================================================================================================
  Script creating date table
  ---***This assumes a Monday week start date
 ===============================================================================================================*/

SET SEARCH_PATH to mdm;
DROP TABLE IF EXISTS mdm.dim_date;


CREATE TABLE dim_date (
  "dim_date_key"          INTEGER                     NOT NULL PRIMARY KEY,
  -- DATE
  "date"             DATE                        NOT NULL,
  "us_format_date"        CHAR(10)                    NOT NULL,
  -- YEAR
  "year"           SMALLINT                    NOT NULL,
  "week_of_year_number"      SMALLINT                    NOT NULL,
  "day_of_year_number"       SMALLINT                    NOT NULL,
  -- QUARTER
  "quarter"            SMALLINT                    NOT NULL,
  "quarter_rank"        INTEGER                     NOT NULL,
  "day_of_quarter_number" INTEGER                   NOT NULL,
  "days_in_quarter"         INTEGER                 NOT NULL,
  "days_until_end_of_quarter" INTEGER               NOT NULL,
  -- MONTH
  "month_number"          SMALLINT                    NOT NULL,
  "month_name"            CHAR(9)                     NOT NULL,
  "month_name_short"      CHAR(3)                     NOT NULL,
  "day_of_month_number"      SMALLINT                    NOT NULL,
  "month_rank"            INTEGER                     NOT NULL,
  "days_in_month"         INTEGER                     NOT NULL,
  "days_until_end_of_month" INTEGER                   NOT NULL,
  -- WEEK
  "day_of_week_number"       SMALLINT                    NOT NULL,
  "first_day_of_week"     DATE                       NOT NULL,
  "last_day_of_week"     DATE                       NOT NULL,
  "week_rank"           INTEGER                     NOT NULL,
  -- DAY
  "day_name"              CHAR(9)                     NOT NULL,
  "day_name_short"        CHAR(3)                     NOT NULL,
  "day_is_weekday"        SMALLINT                    NOT NULL,
  "day_is_weekend"        SMALLINT                    NOT NULL,
  "day_is_last_of_month"  SMALLINT                    NOT NULL,
  "month_start_date"  DATE                          ,
  "month_end_date"  DATE                            ,
  "day_rank"              INTEGER                    NOT NULL,
  "year_month"            CHAR(10)                    NOT NULL,
  "year_month_number"      INTEGER                   NOT NULL
) DISTSTYLE ALL SORTKEY (dim_date_key);


-- TRUNCATE mdm.dim_date

INSERT INTO mdm.dim_date (
    SELECT     to_char(datum, 'YYYY') ::SMALLINT * 10000
               + to_char(datum, 'MM') ::SMALLINT * 100
               + to_char(datum, 'DD') ::SMALLINT                    AS dim_date_key,
           ---------------------------------DATE--------------
           datum                                                    AS date,
           TO_CHAR(datum, 'MM/DD/YYYY') :: CHAR(10)                 AS us_format_date,
           ---------------------------------YEAR--------------
           cast(extract(YEAR FROM datum) AS SMALLINT)               AS year,
           cast(extract(WEEK FROM datum) AS SMALLINT)               AS week_of_year_number,
           cast(extract(DOY FROM datum) AS SMALLINT)                AS day_of_year_number,
           ---------------------------------Quarter--------------
           cast(to_char(datum, 'Q') AS SMALLINT)                    AS quarter,
           1                                                     AS quarter_rank,
           1                                                     AS day_of_quarter_number,
           1                                                     AS days_in_quarter,
           1                                                     AS days_until_end_of_quarter,
           ---------------------------------Month--------------
           cast(extract(MONTH FROM datum) AS SMALLINT)              AS month_number,
           to_char(datum, 'Month')                                  AS month_name,
           LEFT(to_char(datum, 'Month'), 3)                         AS month_name_short,
           cast(extract(DAY FROM datum) AS SMALLINT)                AS day_of_month_number,
           1                                                        AS month_rank,
           extract(DAY FROM DATE_ADD('seconds' , -1 ,
           DATE_ADD('month' , 1 , date_trunc('month',datum))))
                                                                    AS days_in_month,
           days_in_month -  day_of_month_number                        AS days_until_end_of_month,
           ---------------------------------Week--------------
           cast(to_char(datum - interval '1 day', 'D') AS SMALLINT) AS day_of_week_number,
           date_trunc('week' , datum)       AS first_day_of_week,
           date_trunc('week' , datum) + interval '6 days'   AS last_day_of_week,
           1                                                        AS week_rank,

           ---------------------------------DAY--------------
           to_char(datum, 'Day')                                    AS day_name,
           LEFT(to_char(datum, 'Day'), 3)                           AS day_name_short,
           CASE WHEN day_of_week_number IN (6,7)
                   THEN 0 ELSE 1 END                                AS day_is_weekday,
           CASE WHEN day_of_week_number IN (6,7)
           THEN 1 ELSE 0 END                                AS       day_is_weekend,
           CASE WHEN day_of_month_number = days_in_month THEN
                1 ELSE 0 END                                        AS day_is_last_of_month,
           -----Month Start and END -------
           CASE WHEN day_of_month_number =1
                THEN date ELSE NULL END                            AS month_start_date,
           CASE WHEN day_is_last_of_month = 1
               THEN date ELSE NULL END                             AS month_end_date,

                cast(seq + 1 AS INTEGER)                            AS day_rank,
           ---------------------------------Year Month--------------
          to_char(datum, 'YYYY-MM')   :: CHAR(10)                         AS year_month,
           to_char(datum, 'YYYY') ::SMALLINT * 100
               + to_char(datum, 'MM') ::SMALLINT                 AS year_month_number
    FROM

        (
            SELECT '2013-01-01' :: DATE + seq  AS datum,
                   seq
            FROM mdm.seed_series ---seed up to 73000 or 20 * 365
        ) as sequence
        ---test
    WHERE 1 = 1
    ORDER BY 1
);


---------------------------------------------
---------------------------------------------
--UPDATE WEEK_RANK
---------------------------------------------
---------------------------------------------
UPDATE mdm.dim_date
SET week_rank = week_rank_update.week_rank
FROM mdm.dim_date dd
JOIN
(
SELECT
year
,week_of_year_number
,RANK() OVER(ORDER BY year, week_of_year_number) as week_rank
FROM (
         SELECT DISTINCT year
                       , week_of_year_number
         FROM mdm.dim_date
     )tmp
 )week_rank_update
ON dd.year = week_rank_update.year
AND dd.week_of_year_number = week_rank_update.week_of_year_number;

---------------------------------------------
---------------------------------------------
--UPDATE MONTH_RANK
---------------------------------------------
---------------------------------------------
UPDATE mdm.dim_date
SET month_rank = month_rank_update.month_rank
FROM mdm.dim_date dd
JOIN
(
SELECT
year
,month_number
,RANK() OVER(PARTITION BY 1 ORDER BY year, month_number) as month_rank
FROM
(
SELECT DISTINCT
year
,month_number
FROM mdm.dim_date
)tmp
)month_rank_update
ON dd.year = month_rank_update.year
AND dd.month_number = month_rank_update.month_number;

---------------------------------------------
---------------------------------------------
--UPDATE Month_Start_Date + Month_End_Date
---------------------------------------------
---------------------------------------------

UPDATE mdm.dim_date
SET month_start_date = month_start_end_update.month_start_date
    ,month_end_date = month_start_end_update.month_end_date
FROM mdm.dim_date dd
JOIN
(
SELECT
date
,last_value(month_start_date) IGNORE NULLS OVER (
ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as month_start_date
,last_value(month_end_date) IGNORE NULLS OVER (
ORDER BY date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as month_end_date
FROM mdm.dim_date
)month_start_end_update
ON dd.date = month_start_end_update.date;

---------------------------------------------
---------------------------------------------
--UPDATE QUARTER_RANK
---------------------------------------------
---------------------------------------------
UPDATE mdm.dim_date
SET quarter_rank = quarter_rank_update.quarter_rank
FROM mdm.dim_date dd
JOIN
(
SELECT
year
,quarter
,RANK() OVER(ORDER BY year,quarter) as quarter_rank
FROM
(
SELECT DISTINCT
year
,quarter
FROM mdm.dim_date
    ORDER BY 1,2
)tmp
) quarter_rank_update
ON dd.year = quarter_rank_update.year
AND dd.quarter = quarter_rank_update.quarter;

---------------------------------------------
---------------------------------------------
--UPDATE day_of_quarter
---------------------------------------------
---------------------------------------------
UPDATE mdm.dim_date
SET day_of_quarter_number = updates.day_of_quarter_number
, days_in_quarter=updates.days_in_quarter
FROM mdm.dim_date dd
JOIN
(
SELECT
date
,CASE
WHEN month_name = 'January' THEN day_of_month_number
WHEN month_name = 'February'  THEN day_of_month_number + 31
WHEN month_name = 'March' THEN day_of_month_number + 59 + is_leap_year
WHEN month_name = 'April' THEN day_of_month_number
WHEN month_name = 'May' THEN day_of_month_number + 30
WHEN month_name = 'June' THEN day_of_month_number + 61
WHEN month_name = 'July' THEN day_of_month_number
WHEN month_name = 'August' THEN day_of_month_number + 31
WHEN month_name = 'September' THEN day_of_month_number + 62
WHEN month_name = 'October' THEN day_of_month_number
WHEN month_name = 'November' THEN day_of_month_number + 31
WHEN month_name = 'December' THEN day_of_month_number + 61
END as day_of_quarter_number
,CASE
WHEN month_name = 'January' THEN 90 + is_leap_year
WHEN month_name = 'February'  THEN 90 + is_leap_year
WHEN month_name = 'March'  THEN 90 + is_leap_year
WHEN month_name = 'April' THEN 91
WHEN month_name = 'May' THEN 91
WHEN month_name = 'June' THEN 91
ELSE 92
END as days_in_quarter
FROM
(SELECT
date
,month_name
,day_of_month_number
,CASE WHEN year % 4 = 0 THEN 1 ELSE 0 END as is_leap_year
FROM mdm.dim_date
)leap
) updates
ON dd.date = updates.date;


---------------------------------------------
---------------------------------------------
--UPDATE days_until_end_of_quarter
---------------------------------------------
---------------------------------------------
UPDATE mdm.dim_date
SET days_until_end_of_quarter = days_in_quarter -  day_of_quarter_number;



--Check
-- SELECT date ,month_start_date , month_end_date ,days_in_month , day_of_month_number ,day_is_last_of_month FROM mdm.dim_date
-- WHERE date BETWEEN '2019-12-15' AND '2020-04-15'
-- SELECT COUNT(1) FROM mdm.dim_date
-- SELECT * FROM mdm.dim_date


