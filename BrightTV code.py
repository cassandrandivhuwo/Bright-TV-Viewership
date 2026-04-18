# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC
# MAGIC ---View the data in the viewership table
# MAGIC SELECT * 
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` 
# MAGIC LIMIT 100;
# MAGIC
# MAGIC --- View the data in the user profiles table
# MAGIC SELECT * 
# MAGIC FROM `workspace`.`case_study2`.`user_profiles_6` 
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ---Join the 2 tables using left join to return all with the viwership as the left table and user profiles as right table
# MAGIC SELECT * 
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC LIMIT 100;
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC ---EDA
# MAGIC ---Check the number of TV users
# MAGIC ---The number of TV users is 10000
# MAGIC SELECT COUNT(A.UserID0) As number_of_users
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ---Check the number of channels
# MAGIC ---The number of channels is 10000
# MAGIC SELECT COUNT (A.Channel2) As number_of_channels
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC LIMIT 100;
# MAGIC
# MAGIC --------------------------------------------Check for NULL values in the columns
# MAGIC ---Check for null values in the categorical columns of interest
# MAGIC ---No NULL values
# MAGIC SELECT * 
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC WHERE A.Channel2 IS NULL OR B.Gender IS NULL OR B.Race IS NULL OR B.Province IS NULL
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ---Check for null values in the quantitative columns of interest
# MAGIC ---No NULL values
# MAGIC SELECT * 
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC WHERE A.UserID0 IS NULL OR B.UserID IS NULL OR B.Age IS NULL 
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ---Check for null values in the date column
# MAGIC ---No NULL values 
# MAGIC SELECT * 
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC WHERE A.RecordDate2 IS NULL  
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ----------------------------------- Check the unique values in the columns
# MAGIC ---4386 Unique viewers
# MAGIC SELECT COUNT(DISTINCT A.UserID0) As number_of_viewers
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ---Check unique channel categories
# MAGIC ---There are 21 channels
# MAGIC SELECT DISTINCT A.Channel2 
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC LIMIT 100;
# MAGIC
# MAGIC ---There are 3 genders: Female, Male and None
# MAGIC SELECT DISTINCT B.Gender
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID;
# MAGIC
# MAGIC ---There are 7 races: indian_asian, other, None, black, coloured, white and blank space " "
# MAGIC SELECT distinct B.Race
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID;
# MAGIC
# MAGIC ---There are 10 provinces: Gauteng,Limpopo,Eastern Cape,Kwazulu Natal,None,North West,Free State,Mpumalanga,Western Cape and Northern Cape
# MAGIC SELECT distinct B.Province
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID;
# MAGIC
# MAGIC -------------------------------------------------Check the date range
# MAGIC ---There are 4 months data records, colleted from 2016-01-01 to 2016-04-01
# MAGIC SELECT MIN (DATE(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'))) AS min_date,
# MAGIC MAX (DATE(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'))) as max_date
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID;
# MAGIC
# MAGIC -----------------------------------------------Check the minimum, max and average Age
# MAGIC ---The minimum age is 0, maximum age is 114, and the average age is 32.227
# MAGIC SELECT MIN(B.Age) AS youngest_age,
# MAGIC MAX(B.Age) AS oldest_age,
# MAGIC AVG(B.Age) AS average_age
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID;
# MAGIC
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC --------------------------------------------------Data Wrangling
# MAGIC ---Rename the blank spaces in the Race column to None
# MAGIC ---10 Blank spaces in the Race column
# MAGIC SELECT count(B.Race) as no_of_blanks
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC WHERE B.Race= ' '
# MAGIC LIMIT 10;
# MAGIC
# MAGIC ---1057 None values in Race column
# MAGIC SELECT count(B.Race) as no_of_nones
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC WHERE B.Race='None'
# MAGIC LIMIT 10;
# MAGIC
# MAGIC ---102 Other values in Race column
# MAGIC SELECT count(B.Race) as no_of_other
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC WHERE B.Race='other'
# MAGIC LIMIT 10;
# MAGIC
# MAGIC --- Rename the blank spaces '' to None in the Race column, to show missing or unknown data
# MAGIC UPDATE `workspace`.`case_study2`.`user_profiles_6`
# MAGIC SET Race = 'None'
# MAGIC WHERE TRIM(Race) = '';
# MAGIC
# MAGIC ---Re-check number of None values in the Race column
# MAGIC ---1067 None values in Race column
# MAGIC SELECT count(B.Race) as no_of_nones
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC WHERE B.Race='None'
# MAGIC LIMIT 10;
# MAGIC
# MAGIC
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC ----------------------------------------------Data Processing
# MAGIC ---Convert Date from UTC to SAST 
# MAGIC ---From the Date column extract the day name, month name, day of month and viewing time, Aggregate the data (COUNT the Distinct ID)
# MAGIC ---Create new columns using case statement to enhance the table for better insights (day_classification, age_buckets, time_classification), age_buckets)
# MAGIC --- 8 new columns added: Date, day_name, month_name, day_of_month, viewing time, day_classification, time_classification and age buckets
# MAGIC -----------------------------------------------------
# MAGIC SELECT 
# MAGIC ---Categorical columns
# MAGIC       A.Channel2,
# MAGIC       B.Gender,
# MAGIC       B.Race,
# MAGIC       B.Province,
# MAGIC ---Date and Time
# MAGIC       DATE(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS Record_Date,
# MAGIC       Dayname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS day_name,
# MAGIC       Monthname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS month_name,
# MAGIC       Dayofmonth(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) AS day_of_month,
# MAGIC       date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss')  AS viewing_time,
# MAGIC           
# MAGIC       CASE
# MAGIC             WHEN Dayname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) IN('Sun','Sat') THEN 'Weekend'
# MAGIC             ELSE 'Weekday'
# MAGIC       END AS day_classification,
# MAGIC            
# MAGIC       CASE
# MAGIC             WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss') BETWEEN '05:00:00' AND '11:59:59' THEN '1.Morning'
# MAGIC             WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss') BETWEEN '12:00:00' AND '17:59:59' THEN '2.Afternoon'
# MAGIC             WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss')  BETWEEN '18:00:00' AND '21:59:59' THEN '3.Evening'
# MAGIC             ELSE '4.Night'
# MAGIC         END AS time_buckets,
# MAGIC          
# MAGIC --- Age buckets
# MAGIC       CASE
# MAGIC             WHEN B.Age = 0 THEN '1.New Born'
# MAGIC             WHEN B.Age BETWEEN 1 AND 14 THEN '2.Kids'
# MAGIC             WHEN B.Age BETWEEN 15 AND 17 THEN '3.Teens'
# MAGIC             WHEN B.Age BETWEEN 18 AND 34 THEN '4.Youth'
# MAGIC             WHEN B.Age BETWEEN 35 AND 64 THEN '5.Adults'
# MAGIC            ELSE '6.Elder'
# MAGIC       END AS age_buckets,
# MAGIC          
# MAGIC --- COUNTS of ID
# MAGIC       COUNT(DISTINCT A.UserID0) AS number_of_viewers
# MAGIC      
# MAGIC FROM `workspace`.`case_study2`.`viewership_6` AS A 
# MAGIC LEFT JOIN `workspace`.`case_study2`.`user_profiles_6` AS B
# MAGIC ON A.UserID0 = B.UserID
# MAGIC GROUP BY A.Channel2,
# MAGIC       B.Gender,
# MAGIC       B.Race,
# MAGIC       B.Province,
# MAGIC       DATE(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')),
# MAGIC       Dayname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')),
# MAGIC       Monthname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')),
# MAGIC       Dayofmonth(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')),
# MAGIC       date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss'),
# MAGIC     ---Date & time      
# MAGIC       CASE
# MAGIC             WHEN Dayname(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg')) IN('Sun','Sat') THEN 'Weekend'
# MAGIC             ELSE 'Weekday'
# MAGIC       END,
# MAGIC            
# MAGIC       CASE
# MAGIC             WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss') BETWEEN '05:00:00' AND '11:59:59' THEN '1.Morning'
# MAGIC             WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss') BETWEEN '12:00:00' AND '17:59:59' THEN '2.Afternoon'
# MAGIC             WHEN date_format(from_utc_timestamp(A.RecordDate2, 'Africa/Johannesburg'), 'HH:mm:ss')  BETWEEN '18:00:00' AND '21:59:59' THEN '3.Evening'
# MAGIC             ELSE '4.Night'
# MAGIC         END,
# MAGIC          
# MAGIC --- Age buckets
# MAGIC       CASE
# MAGIC             WHEN B.Age = 0 THEN '1.New Born'
# MAGIC             WHEN B.Age BETWEEN 1 AND 14 THEN '2.Kids'
# MAGIC             WHEN B.Age BETWEEN 15 AND 17 THEN '3.Teens'
# MAGIC             WHEN B.Age BETWEEN 18 AND 34 THEN '4.Youth'
# MAGIC             WHEN B.Age BETWEEN 35 AND 64 THEN '5.Adults'
# MAGIC            ELSE '6.Elder'
# MAGIC       END;
# MAGIC
