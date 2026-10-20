**EduSync Analytics Platform**

**Project Overview**

EduSync is an educational data engineering project built using Microsoft Fabric. It processes student, attendance, marks, fee and placement datasets into structured analytical tables for downstream reporting and analytics.

**Architecture**

**Bronze → Silver → Gold**

- **Ingestion:** Parameterized Microsoft Fabric pipelines ingest source CSV datasets into the Lakehouse.
- **Silver transformation:** PySpark notebooks perform data cleaning, validation, standardization and derived-column calculations.
- **Warehouse loading:** A pipeline loads the Silver tables from the Lakehouse into the Fabric Warehouse.
- **Gold transformation:** SQL queries aggregate and transform Silver data into student-level analytical tables.
- **Semantic modelling:** A semantic model organizes the Gold tables for downstream analytics.
- **Ontology:** Entity types, StudentID keys and relationships represent student performance, attendance, fees and placements.

**Technology Stack**

- Microsoft Fabric
- OneLake and Lakehouse
- Data Pipelines and Copy Activity
- Python and PySpark
- Fabric Warehouse and SQL
- Semantic Models
- Fabric Ontology
- GitHub

**Gold Analytical Tables**

- `gold_student_performance`
- `gold_attendance_summary`
- `gold_fee_summary`
- `gold_placement_summary`

The Gold tables were validated to contain 100 student records each, with no duplicate StudentID values.

**Current Status**

**Completed**: Bronze ingestion, Silver transformations, Warehouse loading, SQL-based Gold tables, semantic modelling and ontology entity/relationship configuration.

**Pending**: Power BI dashboard development and further testing of the Ontology Agent Preview. Fabric Data Agent implementation was not completed because of the available trial capacity configuration.

**Repository Contents**

This repository contains the project documentation, exported implementation code, SQL scripts, validation queries and selected screenshots. Fabric workspace resources may need to be recreated or connected separately.

**Notes**

The project uses sample educational datasets. Credentials, access tokens and sensitive account information are not included in this repository.
