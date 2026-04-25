-- Databricks notebook source
---View the data in the viewership table
SELECT * 
FROM `workspace`.`case_study2`.`viewership_6` 
LIMIT 100;

--- View the data in the user profiles table
SELECT * 
FROM `workspace`.`case_study2`.`user_profiles_6` 
LIMIT 100;

---Join the 2 tables using left join to return all with the viwership as the left table and user profiles as right table
SELECT * 
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
LIMIT 100;
-------------------------------------------------------------------------------------------------------------------------------------------------
---EDA
---Check the number of TV users
---The number of TV users is 10000
SELECT COUNT(A.UserID0) As number_of_users
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
LIMIT 100;

---Check the number of channels
---The number of channels is 10000
SELECT COUNT (A.Channel2) As number_of_channels_views
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
LIMIT 100;

--------------------------------------------Check for NULL values in the columns
---Check for null values in the categorical columns of interest
---No NULL values
SELECT * 
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
WHERE A.Channel2 IS NULL OR B.Gender IS NULL OR B.Race IS NULL OR B.Province IS NULL
LIMIT 100;

---Check for null values in the quantitative columns of interest
---No NULL values
SELECT * 
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
WHERE A.UserID0 IS NULL OR B.UserID IS NULL OR B.Age IS NULL 
LIMIT 100;

---Check for null values in the date column
---No NULL values 
SELECT * 
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
WHERE A.RecordDate2 IS NULL  
LIMIT 100;

----------------------------------- Check the unique values in the columns
---4386 Unique viewers
SELECT COUNT(DISTINCT A.UserID0) As number_of_viewers
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
LIMIT 100;

---Check unique channel categories
---There are 21 channels
SELECT COUNT(DISTINCT A.Channel2) AS number_of_channels 
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
LIMIT 100;

---There are 3 genders: Female, Male and None
SELECT DISTINCT B.Gender
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID;

---There are 7 races: indian_asian, other, None, black, coloured, white and blank space " "
SELECT distinct B.Race
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID;

---There are 10 provinces: Gauteng,Limpopo,Eastern Cape,Kwazulu Natal,None,North West,Free State,Mpumalanga,Western Cape and Northern Cape
SELECT distinct B.Province
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID;
-------------------------------------------------Convert date from utc to SAST timestamps
-------------------------------------------------Check the date range
---There are 4 months data records, colleted from 2016-01-01 to 2016-04-01
SELECT MIN (DATE(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'))) AS min_date,
MAX (DATE(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'))) as max_date
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID;

-----------------------------------------------Check the minimum, max and average Age
---The minimum age is 0, maximum age is 114, and the average age is 32
SELECT MIN(B.Age) AS youngest_age,
MAX(B.Age) AS oldest_age,
AVG(B.Age) AS average_age
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID;

-------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------Data Wrangling
---Rename the blank spaces in the Race column to None
---10 Blank spaces in the Race column
SELECT count(B.Race) as no_of_blanks
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
WHERE B.Race= ' '
LIMIT 10;

---1057 None values in Race column
SELECT count(B.Race) as no_of_nones
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
WHERE B.Race='None'
LIMIT 10;

---102 Other values in Race column
SELECT count(B.Race) as no_of_other
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
WHERE B.Race='other'
LIMIT 10;

--- Rename the blank spaces '' to None in the Race column, to show missing or unknown data
UPDATE `workspace`.`case_study2`.`user_profiles_6`
SET Race = 'None'
WHERE TRIM(Race) = '';

---Re-check number of None values in the Race column
---1067 None values in Race column
SELECT count(B.Race) as no_of_nones
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID
WHERE B.Race='None'
LIMIT 10;


-----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------Data Processing
---Convert Date from UTC to SAST 
---From the Date column extract the day name, month name, day of month and viewing time, Aggregate the data (COUNT the Distinct ID)
---Create new columns using case statement to enhance the table for better insights (day_classification, age_buckets, time_classification), age_buckets)
--- 8 new columns added: Date, day_name, month_name, day_of_month, viewing time, day_classification, time_classification and age buckets
-----------------------------------------------------
SELECT 
---Categorical columns
      A.UserID0,
      A.Channel2,
      B.Gender,
      B.Race,
      B.Province,
---Date and Time
      DATE(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS Record_Date,
      Dayname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS day_name,
      Monthname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS month_name,
      Dayofmonth(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS day_of_month,
      date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss')  AS viewing_time,
          
      CASE
            WHEN Dayname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) IN('Sun','Sat') THEN 'Weekend'
            ELSE 'Weekday'
      END AS day_classification,
           
      CASE
            WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss') BETWEEN '05:00:00' AND '11:59:59' THEN '1.Morning'
            WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss') BETWEEN '12:00:00' AND '17:59:59' THEN '2.Afternoon'
            WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss')  BETWEEN '18:00:00' AND '21:59:59' THEN '3.Evening'
            ELSE '4.Night'
        END AS time_buckets,
         
--- Age buckets
      CASE
            WHEN B.Age = 0 THEN '1.New Born'
            WHEN B.Age BETWEEN 1 AND 14 THEN '2.Kids'
            WHEN B.Age BETWEEN 15 AND 17 THEN '3.Teens'
            WHEN B.Age BETWEEN 18 AND 34 THEN '4.Youth'
            WHEN B.Age BETWEEN 35 AND 64 THEN '5.Adults'
           ELSE '6.Elder'
      END AS age_buckets
              
FROM `workspace`.`case_study2`.`viewership_6` AS A 
LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
ON A.UserID0 = B.UserID;
