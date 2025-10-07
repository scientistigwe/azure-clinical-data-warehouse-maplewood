# Azure Clinical Data Warehouse – Project Journal

> Tracks all the practical steps I took for the **Azure Clinical Data Warehouse pipeline project**.

---

## 1. Project Overview

**Goal:** Build an end-to-end pipeline on Azure using **simulated clinical data**.  

**Sources Used:**  
- PostgreSQL → patients, encounters, labs  
- SQL Server → registry, imaging  
- MongoDB → device_readings  
- CSV/JSON → (left out in final scope)

**Architecture:**  
- Ingest → Bronze (Raw) → Silver (Clean) → Gold (Curated) → Analytics (Power BI)

---

## 2. Data Simulation

- Wrote `data_generator.py` using **Faker**.  
- Created datasets: patients, encounters, labs, registry, imaging, device_readings.  
- Added messy data: duplicates, missing values, inconsistent dates.  
- Loaded into Postgres, SQL Server, and MongoDB.

---

## 3. Database Setup

- **Postgres:** patients, encounters, labs (via DBeaver).  
- **SQL Server:** registry, imaging (via DBeaver).  
- **MongoDB:** device_readings (via `mongoimport`).  

---

## 4. Azure Setup

### Step 1 – Resource Group
- Created `rg-azure-clinical-dw-dev` in one region.

### Step 2 – Storage (ADLS Gen2)
- Provisioned storage account with hierarchical namespace.  
- Created containers: `bronze`, `silver`, `gold`.

### Step 3 – Azure Data Factory
- Deployed **ADF** in same region as ADLS.  
- Installed **Self-Hosted Integration Runtime (SHIR)** on local machine.  
- Tested connectivity to Postgres, SQL Server, MongoDB.
- Created linked services for DBs + ADLS.  
- Built **Copy pipelines** to move DB data → Bronze.

### Step 4 – Compute & Transformation
- Set up **Databricks** for Bronze → Silver → Gold.  
- Installed JDBC + Mongo connectors.  
- Ran first Spark job to test read from Bronze.

### Step 5 – Synapse Analytics
- Created **Synapse Workspace**.  
- Connected to ADLS.  
- Verified access to curated (Gold) zone.

### Step 6 – Power BI
- Connected Power BI Desktop → Synapse.  
- Created initial dashboard for patient + lab data.

### Step 7 – Security
- Added **Azure Key Vault** for DB credentials.  
- Assigned RBAC roles for ADF + Databricks.  

---

## 5. Current Status

✅ Databases populated (Postgres, SQL Server, MongoDB).  
✅ ADF pipelines tested for ingestion → ADLS Bronze.  
✅ Databricks tested with sample transformation.  
✅ Synapse + Power BI connected.  

⏳ Next: Build Silver → Gold transformations + more dashboards.

