# Azure Clinical Data Warehouse Pipeline

## Project Description
This project demonstrates a **production-like, end-to-end clinical data warehouse pipeline on Microsoft Azure**.  
It simulates **clinical registry data (FHIR + CSV)**, ingests it into a **data lake (ADLS Gen2)**, processes and normalizes it with **Azure Data Factory + Databricks**, curates data into a **Synapse-based warehouse**, and visualizes insights using **Power BI**.  

The pipeline is designed with **healthcare standards (FHIR, OMOP CDM)** in mind and includes a lightweight **Clinical Decision Support (CDS) demo** inspired by Arden/GEM rules. It also illustrates **governance, de-identification, and security patterns** required for handling sensitive health data.

---

## Objectives
- Build a **cloud-native healthcare data pipeline** on Azure.  
- Demonstrate **bronze → silver → gold** data lakehouse architecture.  
- Simulate **FHIR JSON and registry CSVs** as clinical data sources.  
- Implement **ETL/ELT transforms** using Databricks/Synapse.  
- Curate data into **analytics-ready warehouse schemas** (OMOP-inspired).  
- Deliver **visual insights** via Power BI reports.  
- Showcase a **CDS rules engine microservice** for patient alerts.  
- Follow **production best practices**: infra as code, monitoring, security, Purview governance.

---

## Architecture
```mermaid
flowchart LR
  A["Simulated Clinical Sources (FHIR JSON, CSV)"] -->|ADF / Function| B["Azure Data Lake Gen2 - Bronze"]
  B --> C["Databricks / ADF - Transform to Silver"]
  C --> D["Curated Gold Layer - ADLS / Delta Tables"]
  D --> E["Azure Synapse SQL - Curated Warehouse"]
  E --> F["Power BI Reports - Visual Analytics"]
  E --> G["CDS Engine (Arden/GEM Rules, CDS Hooks)"]

  subgraph Governance_and_Security
    B & C & D & E --> H["Purview / Monitor / Key Vault / RBAC"]
  end
