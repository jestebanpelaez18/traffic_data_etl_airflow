# Traffic Data ETL Pipeline with Apache Airflow

## Project Description

This project implements an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow and Bash scripting to process toll plaza traffic data from various formats into a unified dataset. The goal is to enable traffic analysis that could help reduce congestion on national highways.

The pipeline extracts data from three different file formats—CSV, TSV, and fixed-width text—then transforms and consolidates the data using simple Bash commands executed via Airflow’s BashOperator. The final output is a clean, comma-separated file stored in a staging area, ready for further analysis or ingestion into a database.


## Pipeline Overview

The Airflow DAG performs the following tasks:

- **Extract CSV Data**: Reads and extracts data from a comma-separated values file.

- **Extract TSV Data**: Reads and extracts data from a tab-separated values file.

- **Extract Fixed-Width Data**: Parses a fixed-width formatted file to extract relevant fields.

- **Consolidate Data**: Merges the extracted data into a single intermediate file.

- **Transform Data**: Replaces delimiters and formats the data for loading.

- **Load Data**: Writes the cleaned and transformed data to the staging area.