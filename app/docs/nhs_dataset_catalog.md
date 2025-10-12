# NHS DATASET CATALOG & DATA GENERATION SPECIFICATIONS
## Complete 14-Day Journey Data Requirements

---

## 📊 **DATASET CATALOG - COMPLETE INVENTORY**

### **PRIMARY NHS NATIONAL DATASETS (DSCRO Level)**

#### **1. SUS+ (Secondary Uses Service Plus)**
**Purpose:** Hospital activity data for commissioning and payment
**Collection Frequency:** Daily/Monthly submissions
**Record Volume:** ~50M records annually (England)
**Key Features:**
- **Admitted Patient Care (APC):** Inpatient episodes and spells
- **Outpatient (OP):** Consultant-led outpatient attendances  
- **Accident & Emergency (A&E):** Emergency department attendances

**Data Elements (200+ fields):**
- Patient demographics (NHS Number, DOB, Gender, Ethnicity, Postcode)
- Episode details (Admission date, Discharge date, Length of stay)
- Clinical coding (Primary/Secondary diagnoses ICD-10, Procedures OPCS-4)
- Healthcare Resource Groups (HRG) for payment grouping
- Provider details (Trust code, Site code, Consultant code)
- Commissioning information (CCG/ICB responsible)

#### **2. ECDS (Emergency Care Data Set)**
**Purpose:** A&E department activity and outcomes
**Collection Frequency:** Daily real-time submissions
**Record Volume:** ~24M attendances annually
**Key Features:**
- Arrival and departure times with minute-level precision
- Triage category and clinical assessment outcomes
- Investigations performed (X-ray, blood tests, CT scans)
- Discharge destination and follow-up arrangements

**Data Elements (150+ fields):**
- Attendance details (Arrival mode, Referral source, Departure method)
- Clinical information (Presenting complaint, Diagnosis, Treatment)
- Quality metrics (Time to treatment, Total time in department)
- Safeguarding flags and special circumstances

#### **3. MHSDS (Mental Health Services Data Set)**
**Purpose:** Mental health service activity and outcomes
**Collection Frequency:** Monthly submissions
**Record Volume:** ~15M contacts annually
**Key Features:**
- Care clusters and payment groupings for mental health
- Outcome measures (HoNOS, GAF, clinical assessments)
- Care pathway tracking from referral to discharge

**Data Elements (300+ fields):**
- Referral information (Source, Urgency, Screening outcomes)
- Care planning (Care clusters, Interventions, Risk assessments)
- Professional contacts (Care Professional Type, Contact duration)
- Outcomes (Clinical measures, Recovery indicators, Satisfaction)

#### **4. CSDS (Community Services Data Set)**
**Purpose:** Community health services activity
**Collection Frequency:** Monthly submissions  
**Record Volume:** ~80M contacts annually
**Key Features:**
- Wide range of community services (District nursing, Physiotherapy, etc.)
- Contact-level detail with care setting information
- Safeguarding and vulnerable patient flags

**Data Elements (180+ fields):**
- Service details (Care Professional Type, Care Activity, Contact mode)
- Patient pathway (Referral source, Discharge planning, Onward referral)
- Geographic information (Care setting, Patient location)

#### **5. IAPT (Improving Access to Psychological Therapies)**
**Purpose:** Mental health talking therapy services
**Collection Frequency:** Monthly submissions
**Record Volume:** ~1.5M referrals annually
**Key Features:**
- Therapy type and session tracking
- Clinical outcome measures (PHQ-9, GAD-7, WSAS)
- Waiting times and access metrics

#### **6. NHS BSA Prescribing Data (ePACT2)**
**Purpose:** Primary care prescribing activity and costs
**Collection Frequency:** Monthly submissions
**Record Volume:** ~1.1B prescription items annually
**Key Features:**
- BNF (British National Formulary) classification system
- Practice and prescriber-level data
- Cost and quantity information with detailed breakdowns

**Data Elements (50+ fields):**
- Prescription details (BNF code, Drug name, Strength, Quantity)
- Financial information (Net ingredient cost, Discount, Patient charges)
- Prescriber details (Practice code, Prescriber type)

### **SECONDARY DATA SOURCES (Integration Level)**

#### **7. Diagnostic Imaging Data Set (DIDS)**
**Purpose:** Diagnostic imaging activity across all modalities
**Key Features:**
- Imaging modality (CT, MRI, Ultrasound, X-ray, etc.)
- Referral source and clinical indication
- Reporting timeframes and outcomes

#### **8. Cancer Waiting Times (CWT)**
**Purpose:** Cancer pathway tracking and performance monitoring
**Key Features:**
- 62-day pathway tracking from urgent GP referral
- Treatment modality and outcome tracking
- Staging and histology information

#### **9. Maternity Services Data Set (MSDS)**
**Purpose:** Maternity care pathway from booking to discharge
**Key Features:**
- Antenatal, birth, and postnatal care episodes
- Mother and baby outcome measures
- Intervention and complication tracking

#### **10. Children and Young People's Mental Health (CYPMH)**
**Purpose:** Specialist mental health services for under-18s
**Key Features:**
- CAMHS referrals and interventions
- School-based mental health support
- Crisis intervention and inpatient care

### **LOCAL AUTHORITY & SOCIAL CARE DATA**

#### **11. Adult Social Care Activity and Finance (ASCOF)**
**Purpose:** Social care packages and outcomes
**Key Features:**
- Care package types and costs
- Assessment outcomes and support plans
- Integration with NHS services

#### **12. Housing and Environmental Health Data**
**Purpose:** Social determinants affecting health outcomes
**Key Features:**
- Housing conditions and homelessness
- Environmental health incidents
- Community safety and wellbeing

### **REFERENCE AND LOOKUP DATA**

#### **13. Organisation Data Service (ODS)**
**Purpose:** Master data for all NHS organizations
**Key Features:**
- Trust and practice hierarchies
- Organizational relationships and mergers
- Contact details and operational status

#### **14. National Statistics Postcode Lookup (NSPL)**
**Purpose:** Geographic and demographic enrichment
**Key Features:**
- Index of Multiple Deprivation (IMD) scores
- Lower Layer Super Output Area (LSOA) mappings
- Urban/rural classifications

#### **15. Quality and Outcomes Framework (QOF)**
**Purpose:** GP practice quality indicators
**Key Features:**
- Clinical quality achievement scores
- Practice population demographics
- Prevalence data for long-term conditions

---

## 🎛️ **DATA COMPLEXITY & ANOMALY SPECIFICATIONS**

### **Real-World Data Quality Issues**
- **Missing NHS Numbers:** 5-15% missing or invalid
- **Duplicate Records:** 2-8% depending on dataset
- **Date Inconsistencies:** Discharge before admission (0.5%)
- **Invalid Codes:** Non-existent diagnosis/procedure codes (1-3%)
- **Postcode Variations:** Multiple formats for same location
- **Organizational Changes:** Trust mergers and code changes

### **Clinical Complexity Features**
- **Comorbidity Patterns:** Realistic disease co-occurrence
- **Seasonal Variations:** Flu seasons, holiday patterns, winter pressures
- **Geographic Clustering:** Disease prevalence by area deprivation
- **Demographic Correlations:** Age/gender/ethnicity health patterns

### **Operational Anomalies**
- **Weekend Effects:** Reduced activity and different case mix
- **Bank Holiday Patterns:** Emergency-only services
- **Strike Action:** Reduced elective activity periods
- **COVID-19 Impact:** Dramatic activity changes 2020-2022
- **Cyber Attack Recovery:** Data submission delays and backlogs

---

## 📈 **DATA GENERATION STRATEGY**

### **Phase 1: Reference Data Generation**
1. **Synthetic Patient Population:** 100,000 realistic patients
2. **Provider Network:** 50 trusts, 200 practices, 500 consultants
3. **Geographic Distribution:** Realistic postcode and deprivation mapping
4. **Temporal Framework:** 3 years of activity with seasonal patterns

### **Phase 2: Activity Data Generation**
1. **Patient Journey Simulation:** Realistic care pathways
2. **Clinical Logic:** Evidence-based condition progression
3. **Resource Utilization:** Realistic activity volumes and patterns
4. **Quality Variations:** Different provider performance levels

### **Phase 3: Data Quality Simulation**
1. **Missing Data Patterns:** Realistic missing data scenarios
2. **Error Introduction:** Common data entry and system errors
3. **Correction Processes:** Data quality improvement over time
4. **Audit Trails:** Complete lineage and processing history

This comprehensive dataset catalog provides the foundation for generating realistic NHS data that captures all the complexity and challenges you'll encounter in your role at Arden & GEM.