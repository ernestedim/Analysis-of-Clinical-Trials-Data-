## Clinical Trials Data Analytics Project

---

## **Project Overview**

This project presents an in-depth analysis of over 500,000 clinical trial records sourced from [ClinicalTrials.gov](https://clinicaltrials.gov/ct2/resources/download), covering global research studies across a wide range of medical conditions and study types. Leveraging Spark SQL on Databricks, the analysis addresses four core questions about trial distribution, condition frequency, study duration, and diabetes research trends. The workflow is optimized for large-scale data processing and reproducibility, making it a valuable resource for data scientists and healthcare analysts interested in clinical research patterns.

---

## **Key Questions Addressed**

1. **What is the distribution of clinical trial types (e.g., interventional vs. observational)?**
2. **Which medical conditions are most frequently studied, accounting for multi-condition entries?**
3. **What is the average and median duration of completed clinical trials?**
4. **How has the volume of diabetes-related clinical research changed over time?**

---

## **Data Source**

- **Dataset:** ClinicalTrials.gov (2025)
- **Records:** 522,660 clinical trials
- **Fields:** Study type, status, conditions, funding, start/completion dates, and more
- **Reference:** National Institutes of Health. (2025). ClinicalTrials.gov dataset

---

## **Environment \& Tools**

- **Platform:** Databricks Community Edition
- **Runtime:** 15.4 LTS (Apache Spark 3.5.0, Scala 2.12)
- **Cluster:** 2-core, 15.3 GM Memory driver
- **Libraries:** PySpark, Spark SQL

---

## **Analysis Workflow**

**1. Data Loading \& Validation**

- Loaded the CSV dataset with schema inference and header recognition.
- Verified schema and checked for nulls in critical columns (e.g., Conditions, Study Type, Completion Date).
- Cleaned and standardized date columns for accurate temporal analysis.

**2. Data Preparation**

- Dropped non-essential columns to streamline analysis and improve performance.
- Converted timestamp columns to Spark `DateType`.
- Created a temporary SQL view (`Clinical_trials`) for flexible querying.

**3. Analytical Queries**

- **Trial Type Distribution:** Aggregated and ranked study types by frequency.
- **Top Conditions:** Split multi-condition fields, normalized entries, and identified the ten most common conditions.
- **Trial Duration:** Calculated mean, median, and spread of trial durations (in months) for completed studies.
- **Diabetes Research Trend:** Filtered for completed diabetes-related studies and charted annual trends from 1990–2025, highlighting the impact of external factors like COVID-19[^1].

**4. Visualization**

- Used Spark SQL and Databricks display functions to visualize trends, especially for diabetes-related research over time.

---

## **Key Findings**

- **Interventional studies** are the most prevalent, followed by observational studies.
- **Top studied conditions** include healthy volunteers, breast cancer, obesity, stroke, hypertension, depression, and HIV.
- **Average (mean) trial duration:** ~35.6 months; however, the **median duration** (24.9 months) is more representative due to outliers.
- **Diabetes research** saw a sharp increase post-2000, peaking in 2019, with a notable dip in 2020 likely linked to the COVID-19 pandemic.

---

## **How to Reproduce**

1. **Prerequisites:**
    - Databricks Community Edition account
    - Upload the ClinicalTrials.gov CSV dataset to DBFS or your Databricks workspace
2. **Setup:**
    - Clone this repository
    - Open the notebook in Databricks
    - Adjust the `file_path` variable to match your dataset location
3. **Run the Analysis:**
    - Execute cells sequentially to load data, perform validation, and run each analytical query
    - Visualize results directly in Databricks

---

## **Project Structure**

- `notebook.ipynb` – Main analysis notebook (Spark SQL, PySpark)
- `README.md` – Project overview and instructions
- `Clinicaltrial_16012025.csv` – Raw dataset (not included; download from ClinicalTrials.gov)

---

## **References**

- National Institutes of Health. (2025). ClinicalTrials.gov dataset
- Databricks Runtime Documentation

---

## **Contact**

For questions or collaboration, please contact **Ernest Edim**.

---



<div style="text-align: center">⁂</div>