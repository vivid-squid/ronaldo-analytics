# Ronaldo Shot Analytics Pipeline

An **end-to-end analytics engineering case study** demonstrating how raw event-level data can be transformed into reliable analytical insights using a modern data stack.

This project simulates a **production-style analytics platform**, highlighting key engineering practices such as:

- Structured data ingestion
- Layered warehouse modeling
- Automated data quality validation
- Reproducible transformations
- Analytics-ready semantic models

The system transforms raw event data into structured models that power business insights and visual analytics.

---

## Project Objective

The primary goal of this project is to demonstrate how a modern data engineering pipeline can be built using cloud-native tools while maintaining strong engineering discipline.

This includes answering the following key questions:

- What data was included in analytics and why?
- What data was excluded and why?
- What data quality issues were discovered?
- How were those issues handled?
- What insights can be derived from the cleaned dataset?

The final result is a **reproducible analytics pipeline that transforms raw event data into business-ready insights.**

---

## Architecture Overview

The pipeline follows a modern **ELT architecture**, where raw data is first loaded into the warehouse and then transformed using a dedicated transformation framework.

Local CSV Dataset  
↓  
Azure Data Factory  
(Orchestration & ingestion)  
↓  
Snowflake Data Warehouse  
RAW → CLEAN → ANALYTICS  
↓  
dbt Core  
(Data transformation & testing)  
↓  
Power BI  
(Business insights & visualization)

Each layer in the architecture has a clearly defined responsibility:

- **Azure Data Factory** orchestrates ingestion and pipeline execution  
- **Snowflake** acts as the central storage and compute layer  
- **dbt Core** manages transformation logic and testing  
- **Power BI** provides the business-facing analytics interface

---

# Technology Stack

The project uses a modern analytics engineering stack commonly found in production environments.

| Layer | Technology |
|------|------------|
| Orchestration | Azure Data Factory |
| Data Warehouse | Snowflake |
| Transformation Framework | dbt Core |
| Data Quality | dbt Tests |
| Visualization | Power BI |
| Version Control | Git / GitHub |

Each component is responsible for a specific stage of the pipeline, ensuring a clear separation of concerns.

---

## Repository Structure

The repository is organized to clearly separate project assets from transformation logic.

Ronaldo-Dataset  
│  
├── pics  
│   Dashboard screenshots and project visuals  
│  
├── resources  
│   Supporting files and reference materials  
│  
├── Ronaldo_final.sql  
│   Analytical SQL used during exploration  
│  
└── ronaldo_analytics  
    dbt transformation project  
    │  
    ├── analyses  
    ├── macros  
    ├── models  
    ├── seeds  
    ├── snapshots  
    ├── tests  
    ├── dbt_project.yml  
    └── packages.yml  

The **dbt project is intentionally isolated inside its own directory**, separating transformation logic from supporting project assets.

---

# Data Warehouse Design

The Snowflake warehouse follows a **three-layer architecture**, a common best practice in modern analytics engineering.

## RAW Layer

The RAW layer stores ingested data exactly as received from the source system.

Purpose:

- preserve original source data
- enable reprocessing of pipelines
- maintain lineage and traceability

No transformations are applied in this layer.

---

## CLEAN Layer

The CLEAN layer standardizes and validates the raw dataset.

Typical transformations include:

- removing duplicate records
- validating business keys
- handling null values
- resolving conflicting records

This ensures that the dataset is **consistent and reliable before analytics modeling begins.**

---

## ANALYTICS Layer

The ANALYTICS layer contains **business-ready models** used directly by reporting tools.

These models:

- simplify reporting logic
- standardize business metrics
- prevent BI tools from implementing transformation logic

# Data Quality Controls

Data quality validation is implemented using **dbt tests**.

These automated tests ensure the dataset remains consistent and reliable.

Examples include:

- **not_null** – ensures critical fields are populated
- **unique** – ensures primary keys are not duplicated
- **accepted_values** – validates categorical fields
- **relationships** – verifies foreign key relationships

These tests run as part of the transformation pipeline and help prevent invalid data from reaching the analytics layer.

---

# Analytical Insights

Using the final analytics models, several insights emerge from the dataset.

### Shot Conversion Patterns

Certain shot types exhibit significantly higher goal conversion rates.

### Shot Distance Impact

Goal probability decreases as shot distance increases, confirming expected scoring dynamics.

### Match Context

Goal scoring patterns vary depending on match phase and remaining time.

These insights demonstrate how **clean data modeling enables meaningful sports analytics.**

---

# Dashboard

The Power BI dashboard explores multiple analytical perspectives including:

- shot success rates
- goal distribution across match phases
- shot distance vs scoring probability
- contextual match analysis

Dashboard screenshots are available in the **pics** directory and **ronaldo_analytics.pdf** in the **resources** directory.

---

# 👤 Author

**Barath L**

Data Engineer | MSc Data Science – Trinity College Dublin

Focused on building scalable data pipelines, reliable analytics platforms, and production-ready machine learning systems.

LinkedIn:  
https://www.linkedin.com/in/bn-l/
