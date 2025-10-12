# Azure Clinical Data Warehouse – Project Journal

> This document tracks all the steps, decisions, and work done for the **Azure Clinical Data Warehouse pipeline portfolio project**.

---

## 1. Project Overview

**Goal:** Build a production-like end-to-end clinical data warehouse pipeline on Azure using **simulated data**, covering:

- EHR / FHIR patient and encounter data  
- Lab / observations  
- IoMT / device readings  
- Registry / administrative data  
- Imaging metadata  
- Medications / prescriptions  
- Procedures / surgeries  
- Claims / billing  

**Pipeline Architecture:**  
- Ingest → Bronze / Raw zone → Silver / Cleaned → Gold / Curated → Analytics / Power BI → CDS demo

**Tools & Services:**  
- **Databases:** Postgres, SQL Server, MongoDB (for IoMT)  
- **Flat Files:** CSV (medications, claims)  
- **FHIR JSON:** Procedures  
- **Data Lake:** ADLS Gen2  
- **ETL / Transform:** Azure Data Factory, Databricks  
- **Analytics:** Synapse / Power BI  
- **Governance / Security:** Azure Purview, Key Vault, RBAC

---

## 2. Step 1 – Data Simulation

**Objective:** Generate realistic, messy datasets for all source systems.

**Actions Taken:**

- Created `data_generator.py` using **Python + Faker**.
- Simulated 9 datasets:

| File / Table         | Source System               | Description                                               |
| -------------------- | --------------------------- | --------------------------------------------------------- |
| patients.csv         | EHR (Postgres)              | Patient master table                                      |
| encounters.csv       | EHR (Postgres)              | Patient encounters                                        |
| labs.csv             | Lab DB (Postgres)           | Lab tests / observations                                  |
| device\_readings.csv | IoMT (MongoDB)              | Sensor / device measurements                              |
| registry.csv         | Administrative (SQL Server) | Insurance & demographic info                              |
| imaging.csv          | PACS metadata (SQL Server)  | Imaging study metadata                                    |
| medications.csv      | SQL Server                  | Prescriptions / pharmacy exports                          |
| procedures.json      | MongoDB                     | Procedures and surgeries (FHIR JSON stored as collection) |
| claims.csv           | Postgres                    | Billing and claims (de-identified)                        |

**Decisions / Rationale:**

Decisions / Rationale:

Chose Postgres for patients, encounters, labs, and claims (payer/clinical join analysis).

Chose SQL Server for registry, imaging metadata, and medications (admin + operational data).

Chose MongoDB for IoMT device readings and procedures (JSON structure fits document model).

Designed PatientID as central join key; EncounterID joins labs, imaging, procedures.

Included messy data: missing values, duplicates, inconsistent formats for realism.

Result:

9 datasets generated and imported into one of the three DBs.

Data Quality Issues Introduced:

Missingness, duplicates, type drift, inconsistent date formats.

Schema drift (extra columns), invalid codes/values, referential gaps.

Mixed cases/whitespace inconsistencies.
---

## 3. Step 2 – Database Setup

**Objective:** Import CSVs into respective DBs and organize flat file / JSON sources.

**Plan:**

| File / Table | Target System | Notes |
|--------------|---------------|-------|
| patients.csv | Postgres | Primary patient master table |
| encounters.csv | Postgres | Linked to patients |
| labs.csv | Postgres | Linked via EncounterID + PatientID |
| registry.csv | SQL Server | Admin / demographic data |
| imaging.csv | SQL Server | Metadata only; linked via EncounterID |
| device_readings.csv | MongoDB | Collection with PatientID + timestamp |
| medications.csv | CSV flat file | Pharmacy export (raw source) |
| procedures.json | JSON (FHIR) | Surgical/procedural data (raw source) |
| claims.csv | CSV flat file | De-identified billing/claims data |

---

## Data Import Summary

For the Azure Clinical Data Warehouse simulation, I imported the **database-backed datasets** into MongoDB, PostgreSQL, and SQL Server. The **flat file and JSON datasets** were kept in raw format for ingestion testing.

## 1. MongoDB

**Database Name:** azure_clinical_data_warehouse  
**Collections:** device_readings  
**Tools Used:**
- MongoDB Database Tools (`mongoimport.exe`)  
- Mongo shell (`mongosh`)  

```bash
mongoimport --db clinical_data --collection device_readings --type csv --headerline --file "C:\path\to\device_readings.csv"

"C:\Program Files\MongoDB\Tools\mongodb-database-tools-windows-x86_64-100.13.0\bin\mongoimport.exe" ^
  --db azure_clinical_data_warehouse ^
  --collection device_readings ^
  --file "C:\Users\User\Documents\azure-clinical-data-warehouse\data-sim\procedures.json" ^
  --jsonArray

```

...

## 2. PostgreSQL
(patients, encounters, labs – via DBeaver)  

## 3. SQL Server
(registry, imaging – via DBeaver)  

## 4. Flat Files / JSON
- `medications.csv` and `claims.csv` retained in local folder to simulate external file-based sources.  
- `procedures.json` generated as FHIR-like JSON file, to be ingested via pipeline.  

---


# Azure Platform Setup – Stepwise Order

This section documents the Azure resource setup for the **Clinical Data Warehouse pipeline project**.  
Each step includes **Key Steps / Points / Actions**, with space for specific notes.

---

# Azure Platform Setup – Mindmap

```mermaid
mindmap
  root((Azure Clinical DW Setup))
    Step 1 - Resource Group
      - Create RG
      - Naming convention (rg-azure-clinical-dw-dev)
      - Same region for all services
    Step 2 - Storage (ADLS Gen2)
      - Enable hierarchical namespace
      - Create containers:
        - Bronze (raw data)
        - Silver (cleaned data)
        - Gold (curated data)
      - RBAC & access policies
    Step 3 - Data Integration
      - Azure Data Factory / Synapse Pipelines
      - Self-hosted Integration Runtime
      - Linked services:
        - Postgres
        - SQL Server
        - MongoDB
        - ADLS Gen2
        - Local files (CSV, JSON)
    Step 4 - Compute & Transformation
      - Azure Databricks / Synapse Spark Pools
      - Bronze → Silver cleansing
      - Silver → Gold transformation (OMOP-inspired)
      - Install connectors (JDBC, Mongo, FHIR parser)
    Step 5 - Synapse Analytics
      - Create Synapse Workspace
      - Link ADLS Gen2
      - Serverless or dedicated SQL pools
      - Register gold Delta/external tables
    Step 6 - Power BI
      - Connect to Synapse or ADLS
      - Build dashboards:
        - Patient demographics
        - Lab trends
        - Device telemetry
    Step 7 - Governance & Security
      - Microsoft Purview (catalog + lineage)
      - Azure Key Vault (secrets)
      - RBAC & encryption
    Step 8 - Automation & Monitoring
      - Azure DevOps / GitHub Actions (CI/CD)
      - Azure Monitor + Log Analytics
  end
```
## Step 1 – Create Resource Group

**Key Steps / Points / Actions:**
- Create a dedicated **Resource Group (RG)** for all components of the project.  
- Naming convention: `rg-azure-clinical-dw-dev`.  
- Choose the right **region** (same region for all services to reduce latency + cost).  

**Notes (What I Did):**
- [Add 2–3 lines on how you created it, e.g., via Portal/CLI/ARM].  

---

## Step 2 – Configure Storage (ADLS Gen2)

**Key Steps / Points / Actions:**
- Create **Azure Data Lake Storage Gen2** with **hierarchical namespace enabled**.  
- Create containers for zones:  
  - `bronze` → raw data (direct ingestion from source DBs/CSVs/JSONs).  
  - `silver` → cleaned + normalized data.  
  - `gold` → curated warehouse layer for analytics.  
- Set **access policies / RBAC** for data engineers and pipelines.  

**Notes (What I Did):**
- [Add 2–3 lines: how you provisioned, containers created, RBAC setup].
* Create Storage Account
* Enable ADLS Gen2 (hierarchical namespace)
* Create containers

---

## Step 3 – Data Integration / Ingestion (Azure Data Factory)

**Objective:** Ingest all source data into ADLS Gen2 Bronze layer while ensuring data quality and incremental updates.

**Actions I Undertook:**

1. **Provision Azure Data Factory**
   - I created a new **ADF instance** in my resource group `rg-azure-clinical-dw-dev`.
   - Selected the **same region** as my ADLS Gen2 to reduce latency and avoid egress costs.

2. **Set up Self-hosted Integration Runtime (SHIR)**
   - Installed the SHIR agent (AzureClinicalDWIR) on my local machine to connect securely to:
     - PostgreSQL (patients, encounters, labs)
     - SQL Server (registry, imaging)
     - MongoDB (device readings)
     - Local CSV / JSON files (procedures.json, fhir_observations.json)
   - Verified that SHIR was online and tested connections to all sources.

3. **Create Linked Services**
   - Configured linked services in ADF for each source:
     - Postgres via JDBC → `patients`, `encounters`, `labs`
     - SQL Server via ODBC → `registry`, `imaging`
     - MongoDB → `device_readings` collection
     - Local CSV / JSON → `procedures.json` and `fhir_observations.json`
     - ADLS Gen2 → Bronze container
   - Tested each linked service to ensure connectivity.

4. **Pre-Staging Checks**
   - Before moving data, I ran **validation queries** against each source:
     - Count checks (number of records matches expectations)
     - Schema consistency (columns present and types correct)
     - Missing or malformed keys (e.g., PatientID, EncounterID)
   - Logged results in a local validation report.

5. **Pipelines and Copy Activities**
   - Built ADF **pipelines** for each source dataset.
   - Used **Copy Activity** to move data into the Bronze container.
   - Configured **incremental copy / CDC** for relational sources:
     - Used last-modified timestamps or `ID > last_max_id` logic.
     - For MongoDB, used `Change Streams` to track inserts/updates if needed.
   - Flattened MongoDB JSON for schema compatibility in Bronze.

6. **Minor Transformations**
   - Performed lightweight cleansing during copy:
     - Trimmed whitespace from string fields
     - Converted inconsistent date formats to ISO
     - Corrected numeric types if misformatted

7. **Monitor and Logging**
   - Enabled **ADF monitoring** to track pipeline runs.
   - Created alerts for failed runs and logged metrics to **Azure Monitor**.
   - Verified a few sample rows in Bronze container after each pipeline execution.

## Step 4 – Provision Compute & Transformation

**Key Steps / Points / Actions:**
- Provision **Azure Databricks** (preferred) or **Synapse Spark Pools**.  
- Use for:  
  - Bronze → Silver cleansing.  
  - Silver → Gold transformation (OMOP-inspired schema).  
- Install required connectors (JDBC, MongoDB, FHIR parser if needed).  

**Notes (What I Did):**
- [Add 2–3 lines: which compute chosen, connectors installed, first test job].  

---

## Step 5 – Configure Synapse Analytics

**Key Steps / Points / Actions:**
- Create **Azure Synapse Workspace**.  
- Link **ADLS Gen2** as primary storage.  
- Create **serverless SQL pools** or **dedicated SQL pools** for querying curated data.  
- Register **gold Delta tables** or external tables in Synapse.  

**Notes (What I Did):**
- [Add 2–3 lines: Synapse setup, pool type used, first query tested].  

---

## Step 6 – Connect Power BI

**Key Steps / Points / Actions:**
- Connect **Power BI Service / Desktop** to Synapse SQL endpoint or directly to ADLS Delta tables.  
- Build dashboards:  
  - Patient demographics.  
  - Lab trends and outliers.  
  - Device telemetry over time.  

**Notes (What I Did):**
- [Add 2–3 lines: connection method, first dashboard created].  

---

## Step 7 – Governance & Security

**Key Steps / Points / Actions:**
- Configure **Azure Purview / Microsoft Purview** for data catalog + lineage.  
- Enable **Azure Key Vault** for secrets (DB passwords, API keys).  
- Apply **RBAC** at resource and container levels.  
- Enable **encryption** at rest (default) and in transit (TLS).  

**Notes (What I Did):**
- [Add 2–3 lines: governance setup, access control, encryption confirmation].  

---

## Step 8 – Automation & Monitoring

**Key Steps / Points / Actions:**
- Automate with **Azure DevOps / GitHub Actions** for CI/CD of pipelines & notebooks.  
- Set up **Azure Monitor + Log Analytics** to track ingestion jobs and failures.  

**Notes (What I Did):**
- [Add 2–3 lines: monitoring configuration, first alerts tested].  

---

✅ Logical sequence:  
**RG → Storage → Integration → Compute → Synapse → Power BI → Governance → Automation.**
