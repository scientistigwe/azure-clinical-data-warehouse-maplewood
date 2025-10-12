
# NHS Data Flow Projects – Arden & GEM CSU (Onboarding Practice)

This document describes **two simulated partner projects** designed to help build mastery of Azure data engineering in the NHS context. 
Each project mimics Arden & GEM’s National Flows team work with DSCRO datasets.

---

## 📂 Project 1: CareFlow Insights  
**Partner:** Birmingham & Solihull ICB (Arden & GEM client)  

### Business Case  
Birmingham & Solihull ICB needs to understand how **acute hospital activity (SUS+)** interacts with **prescribing patterns** and **mental health referrals (MHSDS)** across the region. Commissioners suspect that patients with high-cost inpatient episodes are also driving prescribing and MH service costs. The ICB has asked Arden & GEM to develop a prototype data pipeline in Azure to join these flows and deliver insights to support **population health management**.  

### Scope & Deliverables  
- Build an **end-to-end pipeline** in Azure that ingests SUS+, Prescribing, and MHSDS datasets.  
- Pseudonymise patient identifiers for safe sharing.  
- Link datasets to produce **integrated patient-level activity views**.  
- Deliver commissioner-ready Power BI dashboards showing:  
  - High-cost inpatients and their prescribing burden.  
  - MH referrals following inpatient discharge.  
  - Trends in total activity by Trust.  

### Dataset Focus  
- **SUS+ Inpatient Extracts**  
- **NHS BSA Prescribing Data**  
- **MHSDS Referrals & Care Clusters**  

### High-Level Architecture  
1. **Ingestion:** ADF pipelines → ADLS `raw`.  
2. **Transformation:** Databricks notebooks (pseudonymise, map HRGs to tariffs, map BNF to drugs).  
3. **Processing:** Store integrated dataset in ADLS `processed`.  
4. **Warehouse:** Synapse SQL pool → fact & dimension model (`fact_activity`, `fact_prescriptions`, `fact_mhsds`).  
5. **Assurance:** Data quality checks (valid NHS numbers, referral date consistency, HRG tariff validation).  
6. **Presentation:** Power BI dashboards for commissioners.  

### Learning Objective  
- Master SUS+ structure (episodes, HRG, tariff).  
- Practice linking prescribing (BNF) and MH referrals to inpatient spells.  
- Deliver a usable commissioning dashboard pipeline in 1 week.  

---

## 📂 Project 2: Community 360  
**Partner:** Norfolk & Waveney ICS (Arden & GEM client)  

### Business Case  
Norfolk & Waveney ICS are under pressure from NHS England to improve **urgent and community care integration**. They need to see how **A&E attendances (ECDS)** connect to **community service usage (CSDS)** and **diagnostic imaging demand (DIDS)**. Arden & GEM will build a prototype pipeline to demonstrate how these flows can be combined for **demand and capacity planning**.  

### Scope & Deliverables  
- Build a **replica end-to-end pipeline** (same Azure stack as Project 1).  
- Ingest and integrate ECDS, CSDS, and DIDS.  
- Deliver outputs showing:  
  - % of A&E attendances referred into community services.  
  - Imaging demand following A&E admissions.  
  - Community service caseload trends.  

### Dataset Focus  
- **ECDS (Emergency Care)** → attendances, arrival mode, discharge destination.  
- **CSDS (Community Services)** → community contacts, caseload, care setting.  
- **DIDS (Diagnostic Imaging)** → scans, modality, referral source.  

### High-Level Architecture  
1. **Ingestion:** ADF pipelines → ADLS `raw`.  
2. **Transformation:** Databricks (link A&E patients to imaging and community care pathways, pseudonymise IDs).  
3. **Processing:** Store integrated dataset in ADLS `processed`.  
4. **Warehouse:** Synapse SQL pool → fact tables (`fact_ecds`, `fact_csds`, `fact_dids`).  
5. **Assurance:** Checks for missing A&E discharge destinations, invalid imaging dates, orphaned community records.  
6. **Presentation:** Power BI dashboards with pathway funnels (A&E → Imaging → Community).  

### Learning Objective  
- Master **ECDS and CSDS** data flow logic.  
- Practice designing a data model across urgent and community services.  
- Build second end-to-end pipeline faster, proving repeatability and efficiency.  

---

# ✅ Outcome for Onboarding  
- **Week 1 (CareFlow Insights):** Handle core Arden datasets (SUS, Prescribing, MHSDS).  
- **Week 2 (Community 360):** Replicate the pipeline with different flows (ECDS, CSDS, DIDS).  
- **Result:** Two realistic “projects” you can reference in onboarding meetings, showing practical mastery of Arden’s data flows and Azure stack.  
