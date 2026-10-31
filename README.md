Formula 1 Data Engineering Project

## Overview

This project implements a Formula 1 data engineering pipeline using Azure Databricks and the Medallion Architecture. It processes Formula 1 datasets through Bronze, Silver, and Gold layers to prepare structured, analytics-ready data.

## Architecture

The pipeline follows three layers:

- **Bronze:** Ingests and stores raw Formula 1 data.
- **Silver:** Cleans, validates, and transforms the ingested data.
- **Gold:** Produces curated datasets and aggregations for analytical use.

## Technologies

Databricks
- PySpark
- Delta Lake
- Unity Catalog
- SQL
- Cloud storage
- Databricks Workflows

## Key Objectives

- Build a structured data processing pipeline.
- Apply the Medallion Architecture to organize data processing.
- Use PySpark for data transformation.
- Store processed datasets using Delta Lake.
- Prepare curated datasets for analytics.

## Setup

The notebooks are designed for a Databricks environment. Access to the original datasets, storage locations, Unity Catalog configuration, and other required resources may be necessary to execute the pipeline.

## Author

Love Yadav
