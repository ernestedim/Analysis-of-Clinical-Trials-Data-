-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Analysis of clinical trials data from ClinicalTrials.gov  
-- MAGIC
-- MAGIC **By Ernest Edim**
-- MAGIC
-- MAGIC ## **Introduction**
-- MAGIC
-- MAGIC In this analysis we examine clinical trial data gotten from ClinicalTrials.gov, making up over 500,000 records of global research studies. Each entry includes study type, status, conditions investigated, funding sources, and start/Completetion dates. Using Spark SQL on Databricks, we explore four key questions: the distribution of trial types (e.g., interventional vs. observational), the most frequently studied medical conditions (accounting for multi-condition entries), the average duration of completed trials in months, and the historical trend of diabetes-related research.  
-- MAGIC
-- MAGIC **Data Source:** this data is sourced from National Institutes of Health. (2025). ClinicalTrials.gov dataset [Data file]. https://clinicaltrials.gov/ct2/resources/download

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Compute/Cluster Configurations used
-- MAGIC - **Databricks Runtime Version** : 15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)
-- MAGIC - **Driver Type** : Community Optimised 15.3 GM Memory, 2 Cores
-- MAGIC - **Spark Config** : spark.databricks.rocksDB.fileManager.useCommitService false
-- MAGIC - **Environment Variable** : No environment variables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DATA LOADING & INITIAL SETUP
-- MAGIC
-- MAGIC I defined a name for the path holding our csv file initializing it to "file_path", then loaded the csv file into a df clinical_df using spark.read function. I also inferred schema to automatically recognize the data type of each column of the csv dataset. Header was set to be true to automatically detect the first row as the header or column names of the dataframe.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define dataset path For local Databricks
-- MAGIC file_path = "dbfs:/FileStore/tables/Clinicaltrial_16012025.csv"  
-- MAGIC
-- MAGIC # Load CSV with explicit schema inference and header recognition Parameters:
-- MAGIC #   - header=True: First row contains column names  
-- MAGIC #   - inferSchema=True: Automatically detect data types (avoid all strings)
-- MAGIC #   - escape='"': Handle quoted fields properly
-- MAGIC clinical_df = spark.read.option("header", True).option("inferSchema", True).option("escape", '"').csv(file_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC clinical_df.show(5)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Created a dataframe: clinical_df for the clinical trial dataset stored in DBFS. I read the CSV file using spark.read with options to detect the dataset headers, infer data types, and handle quoted fields.
-- MAGIC
-- MAGIC **Result**
-- MAGIC - As shown in the output, the data was loaded successfully inot the clincal_df dataframe and there are 15 columns.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DATA VALIDATION CHECKS 
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Print schema to verify inferred types
-- MAGIC print("SCHEMA VERIFICATION:")
-- MAGIC clinical_df.printSchema()
-- MAGIC
-- MAGIC # Record count validation
-- MAGIC total_records = clinical_df.count()
-- MAGIC print(f"\nTOTAL RECORDS LOADED: {total_records:,}") 
-- MAGIC
-- MAGIC
-- MAGIC # Null check for critical columns
-- MAGIC null_counts = {col: clinical_df.filter(clinical_df[col].isNull()).count() 
-- MAGIC                for col in ["Conditions", "Study Type", "Completion Date"]}
-- MAGIC print("\nNULL VALUE COUNTS:")
-- MAGIC for col, count in null_counts.items():
-- MAGIC     print(f"{col}: {count} ({(count/total_records)*100:.1f}%)")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC I have applied clinical_df.printSchema() to preview the inferred data types and match the output to our expectation. Exception of the date columns where we have timestamp instead of datetype, other seem okay. The right date type will be adjusted subsequently.
-- MAGIC
-- MAGIC **Results**
-- MAGIC   - Total number of rows is 522,660
-- MAGIC   - There are 935 (0.2%) nulll values in the Conditions column
-- MAGIC   - Study Type has 900 Null values
-- MAGIC   - Completion Date accounts for 16671 null values

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### COLUMN DROPPING FOR FOCUSED ANALYSIS
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Columns to remove (unrelated to our 4 core questions)
-- MAGIC columns_to_drop = [
-- MAGIC     "NCT Number",       # Unique identifier - not needed for aggregate analysis
-- MAGIC     "Study Title",      # Free-text field - irrelevant for statistical queries
-- MAGIC     "Acronym",         # Abbreviation - no analytical value
-- MAGIC     "Interventions",   # Not required for condition/type/duration analysis
-- MAGIC     "Sponsor",        # Funder details not in assignment scope
-- MAGIC     "Collaborators",  # Secondary organizations - out of scope
-- MAGIC     "Enrollment",     # Participant count irrelevant to our questions
-- MAGIC     "Study Design"    # Too granular for high-level trends
-- MAGIC ]
-- MAGIC
-- MAGIC # Execute column removal
-- MAGIC # Note: * operator unpacks list into separate arguments
-- MAGIC clinical_df = clinical_df.drop(*columns_to_drop)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC columns_to_drop drops columns that won't be used to answer the focused questions. We will be focused on the conditions, study types, and timelines columns. Removing them makes the dataset lighter, improves performance on our large-scale data, and results in a cleaner schema without losing any essential information. If needed, the original dataset can always be reloaded to recover dropped columns.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DATE FORMAT STANDARDIZATION
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC # Convert timestamp dates to proper DateType for temporal analysis
-- MAGIC # Note: to_date() without format assumes 'yyyy-MM-dd' by default
-- MAGIC clinical_df = clinical_df.withColumn(
-- MAGIC     "Start Date", 
-- MAGIC     to_date("Start Date")  # Converts  to DateType
-- MAGIC ).withColumn(
-- MAGIC     "Completion Date", 
-- MAGIC     to_date("Completion Date")  # converts to date
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Start Date and Completion Date columns are converted from timestamp formats to Spark’s DateType using the to_date() function.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Preview the data
-- MAGIC
-- MAGIC clinical_df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This display output confirms the format of date columns have been updated accordinly

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TEMPORARY TABLE CREATION FOR THE CLINICAL TRIAL DF
-- MAGIC
-- MAGIC This step creates the DataFrame as a temporary SQL view (Clinical_trials), enabling querying via Spark SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create the clinical data as a temporary SQL view
-- MAGIC
-- MAGIC clinical_df.createOrReplaceTempView("Clinical_trials")

-- COMMAND ----------

SELECT * FROM Clinical_Trials;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Q1: List all the clinical trial types (as contained in the Type column of the data) along with their frequency, sorting the results from most to least frequent 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC - "SELECT Study Type" Selects the column Study Type (backticks are used because the column name contains a space).
-- MAGIC - COUNT(*) AS Frequency Counts the number of records for each Study Type and labels this count as Frequency.
-- MAGIC - "FROM Clinical_Trials" Specifies the data source, which is the Clinical_Trials table or view.
-- MAGIC - "WHERE Study Type IS NOT NULL"  Filters out records where the Study Type field is null.
-- MAGIC - "GROUP BY Study Type" Groups the data by each unique Study Type to perform aggregation.
-- MAGIC - "ORDER BY Frequency DESC" Sorts the results in descending order based on the Frequency (most common study types appear first).
-- MAGIC - "LIMIT 20" shows only top 20 results.

-- COMMAND ----------

SELECT 
  `Study Type`, 
  COUNT(*) AS Frequency
FROM 
  Clinical_Trials
WHERE 
  `Study Type` IS NOT NULL 
GROUP BY 
  `Study Type`
ORDER BY 
  Frequency DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Interveventional  and observational study types are the most frequent study types with almost 400,000 and about 120,000 frequency rate respectively.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q2: The top 10 conditions along with their frequency (note, that the Condition column can contain multiple conditions in each row, so you will need to separate these out and count each occurrence separately)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - the sPLIT function splits the Conditions column into individual pieces using the `|` symbol as the delimeter.
-- MAGIC - since multiple conditions are listed together in one row, the Explode condion turns each condition into a separete row.
-- MAGIC - Converts each condition to lowercase and removes extra spaces with TRIM(LOWER(condition))  .
-- MAGIC - and filters out empty or null conditions to be sure only valid conditions are kept.
-- MAGIC - Counts how many times each unique condition shows up in the dataset with SELECT Condition, COUNT(*) AS Frequency.
-- MAGIC - Group By Groups all the same conditions together before counting.
-- MAGIC - Then used Order By to Sort the list to show the most common conditions first.
-- MAGIC - Only shows the top 10 most frequent conditions with LIMIT 10.

-- COMMAND ----------

-- Create a temporary view with one row per individual condition (split on "|")
CREATE OR REPLACE TEMP VIEW exploded_conditions AS
SELECT 
  TRIM(LOWER(condition)) AS Condition                      -- Normalize condition: lowercase and trim spaces
FROM 
  Clinical_Trials
LATERAL VIEW EXPLODE(SPLIT(Conditions, '\\|')) AS condition -- Split the pipe-separated string into individual conditions
WHERE
  condition IS NOT NULL AND condition != '';                -- Exclude nulls and empty strings

-- Aggregate and return the top 10 most frequent conditions
SELECT 
  Condition, 
  COUNT(*) AS Frequency                                     -- Count how often each condition appears
FROM 
  exploded_conditions
GROUP BY 
  Condition
ORDER BY 
  Frequency DESC                                            -- Show most common conditions first
LIMIT 10;                                                   -- Only return the top 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Results**
-- MAGIC   - This trial shows that 10312 trials were healthy persons while breast cancer amounts for almost 8,000 interventions.
-- MAGIC   - Among the top rated condions are obesity, stroke, hypertension, depression, prostate cancer among others. 
-- MAGIC   - HIV is the 9th most occuring condition with about 3,800 conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q3: For studies with an end date, calculate the mean clinical trial length in months.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - COUNT(*) Counts trials that have both a start and completion date, and where completion is after the start.
-- MAGIC - AVG(MONTHS_BETWEEN(...)) Shows the average length of trials in months and i rounded to 2 decimals
-- MAGIC - STDDEV(...) Measures the standard deviation of trial durations.
-- MAGIC - Found the shortest trial using MIN(...)
-- MAGIC - Found the longest trial with MAX(...)
-- MAGIC - To Calculate the median duration, I applied PERCENTILE(..., 0.5) whcih gives the middle value — useful if the data is skewed.
-- MAGIC - I also filtered for: non-null start and completion date, completion date that is after or equal to the start date.
-- MAGIC

-- COMMAND ----------

SELECT
  COUNT(*) AS trials_measured,                                  -- Total number of trials included in the analysis
  ROUND(AVG(MONTHS_BETWEEN(
    `Completion Date`, 
    `Start Date`
  )), 2) AS mean_duration_months,                               -- Average trial duration in months, rounded to 2 decimal places
  ROUND(STDDEV(MONTHS_BETWEEN(
    `Completion Date`, 
    `Start Date`
  )), 2) AS stddev_duration_months,                             -- Standard deviation of trial durations in months
  MIN(MONTHS_BETWEEN(
    `Completion Date`, 
    `Start Date`
  )) AS min_duration_months,                                    -- Minimum trial duration in months
  MAX(MONTHS_BETWEEN(
    `Completion Date`, 
    `Start Date`
  )) AS max_duration_months,                                    -- Maximum trial duration in months
  PERCENTILE(MONTHS_BETWEEN(
    `Completion Date`, 
    `Start Date`
  ), 0.5) AS median_duration_months                             -- Median trial duration in months (50th percentile)
FROM 
  Clinical_Trials                                                -- Source table containing clinical trial records
WHERE 
  `Start Date` IS NOT NULL                                       -- Ensure the trial has a recorded start date
  AND `Completion Date` IS NOT NULL                              -- Ensure the trial has a recorded completion date
  AND `Completion Date` >= `Start Date`;                         -- Only include trials with valid durations (i.e., that didn't end before they started)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC - The number of trials measured is 505019. These are likely the number trials that satisfy the laid out conditions
-- MAGIC - The mean duration of the studies in months is about 35.57. It is believed that due to outliers, this figure may not completely reflect the most suitable average duration in months.
-- MAGIC - MAX duration for a study was recorded to be 110 years (1320 month) which does not appear to be realistic. Again this is a more reason why the Median of this distribution was taken to be more suitable to meaure the average duration than the mean.
-- MAGIC - The Average duration or Median duration is calculated to be **24.9** months i.e. about 2 years.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q4: From the studies with a non-null completion date and a status of ‘Completed’ in the Study Status, calculate how many of these related to Diabetes each year. Display the trend over time in an appropriate visualisation. (For this you can assume all relevant studies will contain an exact match for ‘Diabetes’ or ‘diabetes’ in the Conditions column.)

-- COMMAND ----------

SELECT 
  YEAR(`Completion Date`) AS Completion_Year,         -- Extract the year from the 'Completion Date' column
  COUNT(*) AS Diabetes_Studies                         -- Count the number of studies for each year
FROM 
  Clinical_Trials                                       -- Source table containing clinical trial data
WHERE 
  LOWER(Conditions) LIKE '%diabetes%'                  -- Filter trials where the condition includes 'diabetes' (case-insensitive)
  AND `Study Status` = 'COMPLETED'                     -- Only include studies that have been completed
  AND `Completion Date` IS NOT NULL                    -- Ensure the completion date exists (not NULL)
GROUP BY 
  Completion_Year                                       -- Group the results by the extracted year
ORDER BY 
  Completion_Year;                                      -- Order the results chronologically by year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC This query tracks the number of completed diabetes-related clinical trials over time by extracting the year from the "Completion Date" and filtering for finalized studies with non-null dates. It uses LOWER(Conditions) and LIKE %diabetes% for case-insensitive matching, enabling a year-by-year view of trial trends. While it reveals patterns—such as growth in diabetes research post-2015—it may overcount due to partial matches (e.g., "prediabetes") and lacks granularity regarding trial phases.
-- MAGIC
-- MAGIC **Key Findings**
-- MAGIC   - This analysis shows that diabetes related studies completed between 1989 and 1997 were almost insignificant
-- MAGIC   - It also reveals that there just 1 completed diabetes studies in 2025
-- MAGIC   - 2019 and 2017 appear to have the highest completed diabetes studies with at least 700 studies completed 
-- MAGIC
-- MAGIC **Visualization**
-- MAGIC Our line chart, depicting the trend from 1990 to 2025, shows no significant diabetes studies until 2000. From 2000 to 2010, the number of completed studies steadily increased. However, in 2011, the number dropped by 17. Since then, the completion rate has risen sharply, peaking in 2019, before dropping to 559 in 2020, likely due to the impact of COVID-19. Although there was an attempt at recovery in 2022, 2025 saw a significant decline to an all-time low.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### **Conclusion**
-- MAGIC
-- MAGIC This analysis of over 500,000 clinical trials from ClinicalTrials.gov provided key insights into global medical research trends. Interventional studies were found to be the most common trial type, vastly outnumbering observational ones. By processing multi-condition entries, the most frequently studied conditions included breast cancer, obesity, stroke and a few others. The average clinical trial duration was around 35.6 months, but a more robust median duration of 24.9 months better reflects typical study lengths due to extreme outliers. Lastly, a year-over-year analysis of diabetes-related studies showed significant growth from the early 2000s, peaking in 2019, with a notable decline in 2020 likely due to COVID-19. These findings help contextualize global research priorities and trial dynamics over time.
-- MAGIC
-- MAGIC
-- MAGIC ### **References**
-- MAGIC - Lecture materials and Workshop guides
-- MAGIC - National Institutes of Health. (2025). *ClinicalTrials.gov dataset* [Data file]. Retrieved from [https://clinicaltrials.gov/ct2/resources/download](https://clinicaltrials.gov/ct2/resources/download)
-- MAGIC - Databricks Inc. (2025). *Databricks Runtime 12.2 LTS Documentation*. Retrieved from [https://docs.databricks.com/release-notes/runtime/12.2.html](https://docs.databricks.com/release-notes/runtime/12.2.html)
-- MAGIC - Apache Software Foundation. (2025). *Apache Spark Documentation*. Retrieved from [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)