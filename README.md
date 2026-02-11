# Cubix Data Engineer Training 2025-2026 / Capstone Project

This project demonstrates a full-stack data engineering pipeline using modern tools and best practices. It covers data ingestion, transformation, and analytics using PySpark, Delta Lake, and Azure Data Lake, with a focus on robust engineering concepts and scalable architecture.

## Project Overview

- **Purpose:** End-to-end data engineering pipeline for sales and product analytics
- **Technologies:** Python, PySpark, Delta Lake, Azure Data Lake, Poetry, Git, Databricks
- **Concepts:** Medallion architecture (Bronze/Silver/Gold), Slowly Changing Dimensions (SCD1), star and snowflake schemas, CI/CD, unit testing

## Prerequisites and installation guiadance

### Folder Structure

```
cubix_data_engineer_capstone/
│   README.md
│   pyproject.toml
│   requirements.txt (if present)
│
├── cubix_data_engineer_capstone/
│   ├── etl/
│   │   ├── bronze/   # Raw data ingestion scripts
│   │   ├── silver/   # Cleansed, enriched data transformations
│   │   └── gold/     # Business-level aggregations and metrics
│   └── utils/        # Utility modules for config, datalake, etc.
│
├── tests/            # Unit tests for ETL and utils
└── create_catalogs.ipynb  # Example notebook for Databricks
```


### Apache Spark – Installing PySpark on Windows

1. Create a folder called `spark` on your C: drive (`C:\spark`).
2. Install Java 8. Important: If you already have Java (check with `java -version` in cmd), and it is installed in `Program Files (x86)`, uninstall it and reinstall under `C:\spark\java` (to avoid issues with spaces in the path).
3. Download Spark (choose a version matching your Hadoop version, e.g., 3.3+).
4. Download Hadoop winutils: Go to the winutils GitHub page, click "Code" > "Download ZIP". Extract only the Hadoop version folder you need (e.g., `hadoop-3.3.0`).
5. Copy the Spark and Hadoop folders to `C:\spark`. You should have:
	- `C:\spark\hadoop-3.3.0\bin`
	- `C:\spark\spark-3.5.2-bin-hadoop3\bin`, `conf`, `data`, etc.
6. CreateSystem Environment Variables (Start / Edit the system environment variables): Add four new System variables (the bottom part):
JAVA_HOME, HADOOP_HOME, SPARK_HOME, PYSPARK_PYTHON. The “Variable value” will be their path for the first three, and for PYSPARK_PYTHON it’s “python”

Set environment variables:
```
setx JAVA_HOME C:\spark\java
setx SPARK_HOME C:\spark\spark-3.5.2-bin-hadoop3
setx HADOOP_HOME C:\spark\hadoop-3.3.0
setx PATH "%JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%PATH%"
```
Restart your terminal and verify with `pyspark`.

7. Then go to “Path” still under System Variables and add these three rows: %JAVA_HOME%\bin
%SPARK_HOME% \bin
%HADOOP_HOME%\bin
Click on “OK” to close all windows for the Enviroment Variables.
8. Open a cmd, and type “spark-shell”, if you see similar things, then you’re good to go. If you get an error “spark-shell is not a recognizable command” then maybe you haven’t saved the Environment Variables, check them, and add them again if needed.


### Setting Up Your Development Environment

Follow these steps to prepare your workspace for development:

1. **Install Python 3.10+**
	- Download from the [official Python website](https://www.python.org/downloads/).
	- During installation, check "Add Python to PATH".

2. **Install Poetry**
	- Open a terminal and run:
	  ```sh
	  pip install poetry
	  ```
	- Verify installation with:
	  ```sh
	  poetry --version
	  ```

3. **Clone the Repository**
	- Use Git or VS Code to clone the project:
	  ```sh
	  git clone <repo-url>
	  cd cubix_data_engineer_capstone
	  ```

4. **Install VS Code**
	- Download from [Visual Studio Code](https://code.visualstudio.com/).
	- Recommended extensions: Python, Pylance, Jupyter, GitLens, YAML, and Poetry.

5. **Create and Activate a Virtual Environment**
	- Run:
	  ```sh
	  poetry install
	  ```
	- This will create a virtual environment and install all dependencies from `pyproject.toml`.

6. **Open the Project in VS Code**
	- Open the folder and select the Poetry virtual environment as the Python interpreter (bottom left in VS Code or via Command Palette: "Python: Select Interpreter").

7. **Verify Setup**
	- Run:
	  ```sh
	  poetry run pytest
	  ```
	- All tests should pass, confirming your environment is ready.

---

### Building and Publishing a New Version

After making changes, update and build your package:

1. Bump the version (patch increment):
	```sh
	poetry version patch
	```
	This increases the patch version (e.g., 0.2.24 → 0.2.25).

2. Build the wheel file:
	```sh
	poetry build -f wheel
	```
	This creates a `.whl` file in the `dist/` directory, ready for upload to Databricks or PyPI.

3. Upload the generated `.whl` file to Databricks into your workspace manually (or see `upload_latest_whl.ps1`) and install them in Serverless ipynb notebooks

4. Run Notebooks/scripts



## Medallion Architecture

The Medallion architecture is a layered approach to organizing data pipelines for quality, governance, and scalability. It consists of three main layers:

- **Bronze Layer:** Raw data ingestion, storing unprocessed data as received from source systems. This layer acts as the single source of truth, capturing all incoming data with minimal transformation. It enables traceability and reprocessing if needed.
- **Silver Layer:** Cleansed and conformed data, where business rules, deduplication, and standardization are applied. This layer prepares data for analytics and reporting by ensuring consistency and quality. In this project, the Silver layer implements the SCD1 (Slowly Changing Dimension Type 1) concept, meaning dimension tables always reflect the latest state of the data, overwriting previous values without preserving history.
- **Gold Layer:** Aggregated and business-level data, optimized for analytics, dashboards, and reporting. This layer contains curated datasets and key business metrics, ready for consumption by end users and BI tools.

This project is based on the SCD1 concept, ensuring that dimension tables always show the most current information, with changes overwriting previous values.

### ETL Bronze Layer

The `etl/bronze` layer is responsible for ingesting raw data from various source systems and storing it in its original form. The main purpose of the Bronze layer is to:
- Capture all incoming data with minimal or no transformation
- Preserve the original structure and content for traceability and auditing
- Enable reprocessing and recovery in case of downstream errors

This layer acts as the foundation for all subsequent data processing, ensuring that the pipeline can always be rebuilt from the raw source data if needed.

### ETL Silver Layer

The `etl/silver` folder contains transformation logic for key business entities:

- **calendar.py**: Cleans and standardizes calendar/date data, casting columns to correct types and removing duplicates. Ensures a reliable date dimension for analytics.
- **customers.py**: Transforms raw customer data, applies column mappings, normalizes marital status and gender, creates derived columns (full address, income category, birth year), and removes duplicates for a clean customer dimension.
- **product_subcategory.py**: Maps and filters product subcategory data, renames columns to match schema, and deduplicates records.
- **product_category.py**: Similar to subcategory, this module maps, renames, and deduplicates product category data, supporting multilingual names.
- **sales.py**: Maps and filters sales transaction data, casting and renaming columns, and removing duplicates to ensure a clean fact table for sales analytics.
- **scd.py**: Implements Slowly Changing Dimension (SCD) Type 1 logic using Delta Lake. Efficiently merges new data into master Delta tables, updating changed records and inserting new ones, ensuring dimension tables reflect the latest state without preserving history.

#### Unit Testing for Silver Layer

Unit tests in `tests/etl/silver/` validate the transformation logic for each entity.

Entities:

- **test_calendar.py**: Verifies correct schema, type casting, and deduplication for calendar data.
- **test_customers.py**: Checks column mapping, derived fields, and duplicate removal for customer data.
- **test_product_subcategory.py**: Ensures correct mapping and deduplication for product subcategories.
- **test_product_category.py**: Validates mapping, multilingual support, and deduplication for product categories.
- **test_sales.py**: Confirms correct mapping, type casting, and deduplication for sales transactions.

These tests use PySpark's testing utilities to assert DataFrame equality and schema correctness, ensuring robust, production-ready transformations.

## Running Tests

Run test with command:

```sh
poetry run pytest .\test_<entities>.py
```

or manually within a poetry shell:

pytest .\test_<entities>.py



### ETL Gold Layer

The `etl/gold` layer produces business-level, analytics-ready datasets by aggregating, joining, and enriching data from the Silver layer. The main purpose of the Gold layer is to:
- Provide curated, high-value datasets for reporting, dashboards, and advanced analytics
- Calculate key business metrics and KPIs
- Join and enrich fact tables with descriptive attributes from dimension tables

For example, `wide_sales.py` joins all master data tables (sales, calendar, customers, products, product subcategory, product category) into a single wide fact table, adding business logic and calculated fields. This enables fast, reliable analytics and supports business intelligence use cases.

#### Unit Testing for Gold Layer

- **test_wide_sales.py**: Contains unit tests for the gold ETL logic. It verifies that the join logic in wide_sales.py produces the correct schema and data, and that all calculated and transformed fields (including business metrics and human-readable fields) are correct. The tests use both direct DataFrame comparisons and mocking to ensure robust, isolated validation of the transformation logic.


## Notebooks Overview

- **01_prepare_the_pipeline.ipynb**: Sets up the Databricks environment, installs the project package, and creates required catalogs, schemas, and volumes in the lakehouse. This notebook is the foundation for the pipeline, ensuring all storage and metadata structures are in place before data ingestion.
- **02_ingestion_pipeline.ipynb**: Demonstrates the end-to-end ingestion and transformation process. It imports ETL functions, reads raw data, applies bronze/silver/gold transformations, and writes results to the lakehouse. This notebook is a practical guide for running the pipeline and validating each stage interactively in Databricks.
- **03_homework_ingestion_pipeline.ipynb**: Contains the whole data ingestion process from initial load via the 3 following batches and writing the final results to a parquet file

## Appendix - Spark, Pyspark general information and SQL basics / DWH concepts

### Spark: Advantages & Architecture

- Distributed, in-memory computation for big data
- Handles batch and streaming data
- Fault-tolerant, scalable, supports SQL, Python, R, Scala
- Medallion architecture: Bronze (raw) → Silver (cleaned) → Gold (aggregated)

### Parquet & Delta Tables

- Parquet: Columnar, compressed, efficient for analytics
- Delta Lake: ACID transactions, schema enforcement, time travel, scalable upserts
- Treat Parquet/Delta as queryable tables for fast, reliable analytics

### Spark UI for Debugging

- Use Spark UI to monitor jobs, stages, and tasks
- Identify slow operations, data skews, and code bottlenecks
- Helps eliminate vulnerabilities like OOM errors, inefficient joins, and shuffles

### PySpark vs SQL vs Pandas

- **PySpark:** Distributed, scalable, handles large datasets
- **SQL:** Declarative, easy for analytics, supported in Spark SQL
- **Pandas:** In-memory, best for small/medium data, not distributed

### Handling Parquet Files as Tables

```python
df = spark.read.parquet('path/to/file.parquet')
df.createOrReplaceTempView('my_table')
spark.sql('SELECT * FROM my_table WHERE ...')
```

### SQL Basics (Examples)

#### Most Important SQL Query Types and Clauses

- **SELECT**: Retrieve data from one or more tables
- **WHERE**: Filter rows based on conditions
- **GROUP BY**: Aggregate data by one or more columns
- **HAVING**: Filter groups after aggregation
- **ORDER BY**: Sort results by one or more columns
- **JOIN**: Combine rows from multiple tables (INNER, LEFT, RIGHT, FULL)
- **UNION/UNION ALL**: Combine results from multiple queries
- **INSERT**: Add new rows to a table
- **UPDATE**: Modify existing rows
- **DELETE**: Remove rows from a table
- **CREATE/DROP/ALTER**: Manage table and schema definitions
- **DISTINCT**: Remove duplicate rows from results
- **LIMIT/OFFSET**: Restrict the number of rows returned

### Data Engineering Concepts

- **Data Lake:** Stores raw, semi-structured, and structured data; flexible, scalable
- **Data Warehouse:** Structured, optimized for analytics, strict schema
- **Medallion Architecture:** Layered approach for data quality and governance

### Star vs Snowflake Schema

- **Star:** Fact table at center, denormalized dimension tables
- **Snowflake:** Normalized dimensions, more joins, less redundancy
- **Fact Table:** Quantitative data (e.g., sales)
- **Dimension Table:** Descriptive data (e.g., product, customer)


### SCD types

- **SCD0 (Passive):** No changes allowed after initial load; data is static and never updated.
- **SCD1 (Overwrite):** Overwrites old data with new data, so only the current value is kept. No history is preserved. Used when only the latest state is needed.
- **SCD2 (Full History):** Handles changes in dimension data by creating new records for each change, preserving full history. Each record is versioned (e.g., with effective dates or flags).
- **SCD3 (Limited History):** Tracks limited history by adding new columns for previous values (e.g., previous and current address), allowing comparison of recent changes but not full history.
- **SCD4 (History Table):** Maintains a separate history table to track all changes, while the main dimension table holds only the current value.
- **SCD5 (Mini-Dimension):** Combines SCD1 or SCD2 with a mini-dimension for rapidly changing attributes, reducing the size of the main dimension table.
- **SCD6 (Hybrid):** Combines SCD1, SCD2, and SCD3 to provide both full and limited history, using additional columns and versioning to track changes in multiple ways.

### Poetry & Git, Python Packaging, Unit Tests

- Use Poetry for dependency management and packaging
- Git for version control and collaboration
- Write unit tests in `tests/` to ensure code quality

### CI/CD, Code Review, Databricks Pipeline

- Use CI/CD (e.g., GitHub Actions) for automated testing and deployment
- Code review for quality and security
- Deploy final package to Databricks for production pipelines

deployment.yaml for reference only, is currently disabled as it is not functioning in Free Edition