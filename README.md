# Big Data Tools and Techniques Project: Clinical Trial Data Analysis

## Project Overview

This project involves performing big data analysis on clinical trial datasets from 2020, 2021, and 2023. The main goal is to clean, prepare, and analyze these datasets using the Databricks platform, specifically leveraging Spark (DataFrames and RDDs) and SQL, to answer specific questions about the clinical trials.

## Key Implementation Details and Findings

### Part 1: Steps to Implement the Project

* **Platform:** Databricks Community Edition workspace was used for all analysis.
* **Setup:**
    * A Databricks cluster was created with the most recent runtime (12.2 LTS including Apache Spark 3.3.2, Scala 2.12).
    * A new notebook was created and attached to the configured cluster.
* **Data Loading and Preparation:**
    * Clinical trial datasets (e.g., `clinicaltrial-2023.zip`, `pharma.zip`) were uploaded to Databricks FileStore (`/FileStore/tables/`).
    * Since `dbutils` does not support unzipping directly, zipped files were copied from DBFS to the local file system (`/tmp/`), unzipped using shell commands (`unzip -d`), and then the extracted CSVs were moved back to new directories in DBFS (e.g., `/FileStore/tables/clinicaltrial_2023/`).
    * **Data Cleaning and Transformation:**
        * A Spark session was initialized.
        * An explicit schema was defined using `StructType` and `StructField` to ensure data consistency, treating all entries as strings initially.
        * A User-Defined Function (UDF) was created to clean data by removing delimiters (`/t`), unnecessary commas, leading/trailing quotes, and extra whitespaces. This UDF also ensured consistent row length by padding missing values.
        * Date formats in "Start" and "Completion" columns were standardized using two UDFs:
            * `delete_day_udf`: Removes the day part (YYYY-MM-DD to YYYY-MM) using regular expressions.
            * `format_date_udf`: Parses and formats date strings (e.g., %Y-%m to %b%Y).
        * Original "Start" and "Completion" columns were deleted after creating new, cleaned date columns.
        * A temporary view (`Clinical_trial`) was created for easier SQL querying.

### Part 2: Analysis Questions and Findings

The analysis addressed several key questions, with consistent results across SQL, DataFrame, and RDD implementations.

1.  **Number of Distinct Studies:**
    * **Objective:** Count the number of distinct studies in the dataset.
    * **Assumption:** Each row with a unique "ID" represents a distinct clinical trial.
    * **Result:** There are **483,422 distinct studies** in the dataset.

2.  **Types of Studies and Frequencies:**
    * **Objective:** List all study types (from the "Type" column) along with their frequencies, ordered from most to least frequent.
    * **Result:** The analysis provides an exhaustive inventory of study types and their frequencies, ordered descendingly.

3.  **Top 5 Conditions and Frequencies:**
    * **Objective:** Find the top 5 conditions (from the "Conditions" column) with their frequencies.
    * **Assumption:** Conditions are separated by `|` delimiter in the "Conditions" column.
    * **Methodology:** Used `explode` function to split conditions, then grouped and counted occurrences, filtering out empty conditions.
    * **Result:** The top 5 conditions and their frequencies were identified.

4.  **Top 10 Non-Pharmaceutical Sponsors:**
    * **Objective:** Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they sponsored.
    * **Assumption:** A separate `pharmaceutical_data.csv` file with a "Parent_Company" column contains all pharmaceutical companies.
    * **Methodology:** Identified sponsors from the clinical trial data not present in the pharmaceutical companies list.
    * **Result:** The top 10 non-pharmaceutical sponsors and their respective counts were successfully identified.

5.  **Completed Studies Per Month in 2023 (Visualization and Table):**
    * **Objective:** Plot the number of completed studies for each month in 2023 and provide a table of the values.
    * **Assumption:** Completion dates are in "Completion_Cleaned" (YY-MM format) and status is "COMPLETED".
    * **Methodology:** Filtered for completed studies in 2023, extracted the month, grouped by month, and counted. Visualized using `matplotlib`.
    * **Result:** A table and a bar chart showing the distribution of completed studies by month for 2023 were generated.

### Extra Features: General and Reusable Code

A general and reusable code solution was developed for analyzing `clinicaltrial_2020` and `clinicaltrial_2021` datasets. This code includes:
* Reading data into an RDD.
* Defining schema for DataFrames.
* Implementing a `delete_dir` function for recursive directory deletion in DBFS.
* Processing and cleaning data using UDFs to handle delimiters, quotes, and whitespaces.
* Standardizing date formats.
* Creating temporary views for SQL querying.

## Conclusion

This project successfully demonstrated the use of Databricks, Spark (DataFrames, RDDs), and SQL for comprehensive big data analysis. It covered essential steps from environment setup and data loading to advanced data cleaning, transformation, and querying. The consistent results across different Spark APIs highlight the robustness of the implemented solutions, and the development of reusable code enhances the project's practicality for similar future analyses.
