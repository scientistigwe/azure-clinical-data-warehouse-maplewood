# Maplewood General Hospital - Azure Data Warehouse Pipeline

## Project Description
This project demonstrates a **production-like, end-to-end clinical data warehouse pipeline on Microsoft Azure**.  
It simulates **clinical registry data (FHIR + CSV)**, ingests it into a **data lake (ADLS Gen2)**, processes and normalizes it with **Azure Data Factory + Databricks**, curates data into a **Synapse-based warehouse**, and visualizes insights using **Power BI**.  

The pipeline is designed with **healthcare standards (FHIR, OMOP CDM)** in mind and includes a lightweight **Clinical Decision Support (CDS) demo** inspired by Arden/GEM rules. It also illustrates **governance, de-identification, and security patterns** required for handling sensitive health data.

**Commissioned by:** Maplewood General Hospital - A fictional regional healthcare provider seeking to modernize their data infrastructure for better patient outcomes and operational efficiency.

---

## Global Objective: Addressing Resource Constraints, Patient Flow Inefficiencies, and Rising Costs
Maplewood General Hospital aims to leverage integrated clinical data analytics to tackle critical operational challenges: optimizing limited resources (e.g., beds and staffing), streamlining patient flows across care settings, and controlling escalating healthcare costs through data-driven insights and predictive modeling.

### Specific Objectives by Dataset Focus
- **Bed Capacity and Utilization Optimization** (Trusts, SUS Episodes): Predict occupancy trends, reduce length of stay, and allocate beds efficiently by specialty to address shortages and overcrowding.
- **Emergency Waiting Time Reduction** (ECDS Attendances): Analyze triage categories, treatment delays, and discharge destinations to minimize ED bottlenecks and improve throughput.
- **High-Cost Patient Management** (Patient Journeys, Prescriptions): Identify at-risk patients, optimize prescribing patterns, and track total care costs to prevent readmissions and manage expenses.
- **Care Coordination and Transitions** (MHSDS Referrals, CSDS Contacts, Social Care): Integrate mental health, community services, and social care data to ensure smooth referrals, reduce fragmented care, and enhance patient outcomes.
- **Population Health and Equity Insights** (Patients, Practices): Analyze demographics (e.g., deprivation, ethnicity) and regional data to target preventive care and address access disparities.

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
