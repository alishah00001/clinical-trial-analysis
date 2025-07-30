-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f"/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # No need to mount DBFS root (it's already mounted at `/FileStore`)
-- MAGIC # Function to recursively delete a directory and its contents
-- MAGIC def delete_dir(path):
-- MAGIC   try:
-- MAGIC     for file in dbutils.fs.ls(path):
-- MAGIC       if file.isDir():
-- MAGIC         delete_dir(file.path)  # Recursive call for subdirectories
-- MAGIC       else:
-- MAGIC         dbutils.fs.rm(path, recurse=True)  # Use fs.rm for files in FileStore
-- MAGIC     dbutils.fs.rm(path, recurse=True)  # Delete the empty directory itself
-- MAGIC   except Exception as e:
-- MAGIC     print(f"Error deleting directory {path}: {e}")
-- MAGIC     
-- MAGIC # Specify directories or files to be deleted (replace with your specific paths)
-- MAGIC paths_to_delete = [
-- MAGIC   "/FileStore/tables/pharma-2.zip"
-- MAGIC ]
-- MAGIC
-- MAGIC # Delete each path
-- MAGIC for path in paths_to_delete:
-- MAGIC   delete_dir(path)
-- MAGIC
-- MAGIC # Unmount DBFS (optional, good practice for security)
-- MAGIC # Since we didn't mount anything, this line is not necessary
-- MAGIC
-- MAGIC print("Cleanup completed!")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ls /tmp 

-- COMMAND ----------

---#COPY THE FILES FROM dbfs TO LOCAL FILE SYSTEM

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("/FileStore/tables/clinicaltrial_2023.zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("/FileStore/tables/pharma.zip", "file:/tmp/")

-- COMMAND ----------

---#CHECKING THAT  WHETHER IT IS IN LOCAL FILE SYSTEM

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ls /tmp/

-- COMMAND ----------

---# UNZIPPING THE FILES IN LOCAL FILES SYSTEM

-- COMMAND ----------

-- MAGIC %python
-- MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2023.zip

-- COMMAND ----------

-- MAGIC %python
-- MAGIC unzip -d /tmp/ /tmp/pharma.zip

-- COMMAND ----------

---#NOW CHECKING ITS CONTENT IN LOCAL FILE SYSTEM

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ls /tmp 

-- COMMAND ----------

---#MAKING DIRECTORIES IN DBFS 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("FileStore/tables/clinicaltrial_2023")
-- MAGIC dbutils.fs.mkdirs("FileStore/tables/pharma")

-- COMMAND ----------

--#NOW MOVE THE UNZIPPED FILES FROM LOCAL FILE SYSTEM TO RECENTLY CREATED DIRECTORIES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("file:/tmp/clinicaltrial_2023.csv", "/FileStore/tables/clinicaltrial_2023", True)
-- MAGIC dbutils.fs.mv("file:/tmp/pharma.csv", "/FileStore/tables/pharma", True)

-- COMMAND ----------

--#NOW CHECKING THE FILES CONTENTS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("FileStore/tables/clinicaltrial_2023/")
-- MAGIC dbutils.fs.ls("FileStore/tables/pharma/")

-- COMMAND ----------

---#INITIALIZING SPARK SESSION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType
-- MAGIC
-- MAGIC # Initialize Spark session
-- MAGIC spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()

-- COMMAND ----------

----#LOADING THE FILE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_path = "/FileStore/tables/clinicaltrial_2023/clinicaltrial_2023.csv"
-- MAGIC raw_rdd = spark.sparkContext.textFile(file_path)

-- COMMAND ----------

---#DEFINING SCHEMA

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Study Title", StringType(), True),
-- MAGIC     StructField("Acronym", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Collaborators", StringType(), True),
-- MAGIC     StructField("Enrollment", StringType(), True),
-- MAGIC     StructField("Funder Type", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Study Design", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True),
-- MAGIC ])

-- COMMAND ----------

----#DATA CLEANING

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC def clean_and_pad(parts):
-- MAGIC     # Remove commas, strip quotes and whitespace
-- MAGIC     cleaned_parts = [part.replace(",", "").strip().strip('"') for part in parts]
-- MAGIC     # Pad the row if it has fewer elements than expected
-- MAGIC     if len(cleaned_parts) < 14:
-- MAGIC         cleaned_parts += [""] * (14 - len(cleaned_parts))
-- MAGIC     return cleaned_parts
-- MAGIC
-- MAGIC processed_rdd = raw_rdd.map(lambda line: line.split("\t")).map(clean_and_pad)
-- MAGIC
-- MAGIC
-- MAGIC # Filter out the header if it's the first row and matches expected headers
-- MAGIC header = processed_rdd.first()  # Assuming the first row is the header
-- MAGIC data_rdd = processed_rdd.filter(lambda row: row != header and len(row) == 14)  # Ensure all rows have exactly 14 elements
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC df = spark.createDataFrame(data_rdd, schema=schema)
-- MAGIC df.show()
-- MAGIC
-- MAGIC

-- COMMAND ----------

---#CORRECTING DATE FORMATE USING USER DEFINED FUNCTIONS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, DateType
-- MAGIC from pyspark.sql.functions import col, udf, lit
-- MAGIC import re
-- MAGIC def delete_day(date_str):
-- MAGIC     """Removes the day part from a date string (if present) using regular expressions."""
-- MAGIC     return re.sub(r'(\b\d(4)-\d(2))-\d(2)\b', r'\1', str(date_str))
-- MAGIC def format_date(date_str):
-- MAGIC     """Formats a date string (YYYY-MM) into a more human-readable format (Month Year)."""
-- MAGIC     if date_str:
-- MAGIC         try:
-- MAGIC             date_obj = datetime.strptime(date_str, "%Y-%m")
-- MAGIC             return date_obj.strftime("%b %Y")
-- MAGIC         except ValueError:
-- MAGIC             return date_str
-- MAGIC     else:
-- MAGIC         return date_str
-- MAGIC # Define UDFs
-- MAGIC delete_day_udf = udf(delete_day, StringType())
-- MAGIC format_date_udf = udf(format_date, StringType())
-- MAGIC # Apply UDFs in the correct order
-- MAGIC df = df.withColumn("Start_Cleaned", delete_day_udf(col("Start")))
-- MAGIC df = df.withColumn("Completion_Cleaned", delete_day_udf(col("Completion")))
-- MAGIC df = df.withColumn("Start_Date", format_date_udf(col("Start_Cleaned")))
-- MAGIC df.withColumn("Completion_Date", format_date_udf(col("Completion_Cleaned")))
-- MAGIC

-- COMMAND ----------

--#NOW DELETING PRIVIOUSE(ORIGINAL) DATE FORMATES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.drop("Start")
-- MAGIC df = df.drop("Completion")
-- MAGIC df = df.drop("Start_Date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create temporary view
-- MAGIC df.createOrReplaceTempView("clinical_trials")
-- MAGIC

-- COMMAND ----------

-----1. The number of studies in the dataset. You must ensure that you explicitly check distinct studies.

-- COMMAND ----------



SELECT DISTINCT count(*) FROM clinical_trials

-- COMMAND ----------

----2. You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.

-- COMMAND ----------

SELECT clinical_trials.Type, count(*) as count FROM clinical_trials
GROUP BY clinical_trials.Type
ORDER BY count DESC

-- COMMAND ----------

---3. The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW all_conditions AS 
SELECT explode(split(Conditions, '\\|')) AS condition
FROM clinical_trials;

SELECT TRIM(condition) AS condition, COUNT(*) as count 
FROM all_conditions
WHERE condition != ''
GROUP BY condition
ORDER BY count DESC
LIMIT 5;


-- COMMAND ----------

-----4. Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the Parent Company column contains all possible pharmaceutical companies.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Assuming your data is stored in a CSV file named "pharmaceutical_data.csv"
-- MAGIC file_path = "/FileStore/tables/pharma/pharma.csv"  # Update the file path accordingly
-- MAGIC
-- MAGIC # Read the CSV file into a DataFrame
-- MAGIC pharma= spark.read.option("header", "true").csv(file_path)
-- MAGIC
-- MAGIC # Select only the "Parent_Company" column
-- MAGIC parent_companies = pharma.select("Parent_Company")
-- MAGIC
-- MAGIC # Show the first few rows of the DataFrame
-- MAGIC
-- MAGIC parent_companies.createOrReplaceTempView("parent_companies_temp")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW non_pharma_sponsor AS SELECT Sponsor FROM clinical_trials WHERE Sponsor NOT IN (SELECT Parent_Company FROM parent_companies_temp);
SELECT Sponsor, count(*) as count FROM non_pharma_sponsor
GROUP BY Sponsor
ORDER BY count DESC
LIMIT 10

-- COMMAND ----------

-----5. Plot number of completed studies for each month in 2023. You need to include your visualization as well as a table of all the values you have plotted for each month.

-- COMMAND ----------



-- Use the temporary view "clinical_trials" created by  Python code

SELECT Month, COUNT(*) AS completed_studies
FROM (
  SELECT *,
    SPLIT(Completion_Cleaned, '-')[0] AS Year,
    REPLACE(REPLACE(SPLIT(Completion_Cleaned, '-')[1], ',', ''), '"', '') AS Month
  FROM clinical_trials
  WHERE Status = 'COMPLETED'
) AS processed_data
WHERE Year = '2023'
GROUP BY Month
ORDER BY Month ASC;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC spark = SparkSession.builder.appName("ClinicalTrialsPlot").getOrCreate()
-- MAGIC
-- MAGIC # Assuming "clinical_trials" is the temporary view name created by your Spark code
-- MAGIC
-- MAGIC # Read data from the temporary view into a Spark DataFrame
-- MAGIC df = spark.sql("SELECT * FROM clinical_trials")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import split, regexp_replace
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Ensure columns are properly formatted
-- MAGIC for col in df.columns:
-- MAGIC     df = df.withColumnRenamed(col, col.strip(",").strip('"'))
-- MAGIC
-- MAGIC # Extract data for completed clinical trials in 2023
-- MAGIC completed_cd = df.withColumn('Year', split('Completion_Cleaned', "-")[0]) \
-- MAGIC                  .withColumn('Month', split('Completion_Cleaned', "-")[1]) \
-- MAGIC                  .withColumn('Month', regexp_replace("Month", ",", "")) \
-- MAGIC                  .withColumn('Month', regexp_replace("Month", '"', "")) \
-- MAGIC                  .filter(df.Status.isin(["COMPLETED"])) \
-- MAGIC                  .filter(df.Completion_Cleaned.startswith("2023")) \
-- MAGIC                  .select("Month", "Year", "Status")
-- MAGIC
-- MAGIC
-- MAGIC completed_cd.filter(completed_cd.Year.isin(["2023"])).groupBy("Month").count().orderBy("Month", ascending=True).show()
-- MAGIC monthly_counts = completed_cd.groupBy("Month").count().orderBy("Month", ascending=True).collect()
-- MAGIC
-- MAGIC # Extract months and counts into separate lists
-- MAGIC months = [row["Month"] for row in monthly_counts]
-- MAGIC counts = [row["count"] for row in monthly_counts]
-- MAGIC
-- MAGIC # Create the plot
-- MAGIC plt.figure(figsize=(10, 6))  # Adjust figure size as desired
-- MAGIC plt.bar(months, counts, color='skyblue')
-- MAGIC plt.xlabel("Month")
-- MAGIC plt.ylabel("Number of Completed Trials")
-- MAGIC plt.title("Completed Clinical Trials per Month (2023)")
-- MAGIC plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
-- MAGIC plt.tight_layout()
-- MAGIC
-- MAGIC # Display the plot
-- MAGIC plt.show()
-- MAGIC
-- MAGIC

-- COMMAND ----------

--Additional Analysis 3: Correlation Between Trial Size and Sponsor's Funding Type

-- COMMAND ----------

--Additional Analysis 3: Correlation Between Trial Size and Sponsor's Funding Type
SELECT `Funder Type`, AVG(Enrollment) AS avg_enrollment
FROM clinical_trials
GROUP BY `Funder Type`
ORDER BY avg_enrollment DESC;

-- COMMAND ----------

---additional analysis

-- COMMAND ----------

--additional analysis this query calculates the total enrollment for each sponsor and orders the results by total enrollment in descending order.

-- COMMAND ----------


SELECT sponsor, SUM(CAST(enrollment AS INT)) AS total_enrollment
FROM clinical_trials
GROUP BY sponsor
ORDER BY total_enrollment DESC
LIMIT 10;


-- COMMAND ----------


