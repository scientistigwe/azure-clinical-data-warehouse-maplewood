#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
data_generator.py

Generates large, messy, and realistic simulated clinical data for multi-source pipelines:
1) EHR / Patients & Encounters
2) Labs / Observations
3) Device / IoMT readings
4) Registry / Admin
5) Imaging metadata
6) Medications (CSV)
7) Procedures (FHIR-like JSON)
8) Claims (CSV)
9) FHIR-like Bundles (Patients, Encounters, Observations)
10) Mongo-ready JSONL (per collection)

Adds UK NHS numbers (valid Mod 11, some invalid), data quality issues, duplicates, type drift, and schema drift.

Outputs:
- CSVs: patients.csv, encounters.csv, labs.csv, device_readings.csv, registry.csv, imaging.csv, medications.csv, claims.csv
- JSONL/NDJSON: patients.jsonl, encounters.jsonl, labs.jsonl, device_readings.jsonl, registry.jsonl, imaging.jsonl
- Mongo JSONL: mongo_<collection>.jsonl
- FHIR-like JSON: procedures.json, fhir_patients.json, fhir_encounters.json, fhir_observations.json
"""

import csv
import json
import random
import uuid
import math
from datetime import datetime, timedelta, timezone
from faker import Faker

# ---------------------------
# Config
# ---------------------------
fake = Faker("en_GB")

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Sizes (increase to scale)
NUM_PATIENTS = 2000
MAX_ENCOUNTERS = 8
MAX_LABS_PER_ENC = 8
MAX_DEVICE_READINGS_PER_PAT = 30
MAX_IMAGING_PER_ENC = 3
MEDS_PER_PAT_RANGE = (1, 6)
PROCS_PER_PAT_RANGE = (1, 4)
CLAIMS_PER_PAT_RANGE = (1, 3)

# Data quality knobs
MISSING_PROB = 0.08
DUPLICATE_ROW_PROB = 0.03
NEAR_DUPLICATE_EDIT_PROB = 0.02
SCHEMA_DRIFT_PROB = 0.02
TYPE_DRIFT_PROB = 0.06
FORMAT_DRIFT_PROB = 0.10
INVALID_NHS_PROB = 0.03
BAD_REF_PROB = 0.02
OUTLIER_PROB = 0.02
FUTURE_DATE_PROB = 0.01

# ---------------------------
# Helpers: dates, formats, drift
# ---------------------------
def random_past_date(days_back=365 * 90, min_back=0):
    d = random.randint(min_back, days_back)
    dt = datetime.now() - timedelta(days=d)
    return dt

def random_recent_timestamp(days=60):
    # Some with timezones, some naive
    ts = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, days),
        seconds=random.randint(0, 86400)
    )
    if random.random() < FORMAT_DRIFT_PROB:
        return ts.isoformat()
    return ts.replace(tzinfo=None)

def maybe_missing(v, p=MISSING_PROB):
    return v if random.random() > p else ("" if random.random() < 0.5 else None)

def maybe_whitespace(v):
    if v is None or v == "":
        return v
    if random.random() < FORMAT_DRIFT_PROB:
        return f" {v} " if random.random() < 0.5 else f"{v}\t"
    return v

def maybe_case(v):
    if not isinstance(v, str):
        return v
    if random.random() < FORMAT_DRIFT_PROB:
        return v.upper() if random.random() < 0.5 else v.lower()
    return v

def maybe_type_drift(v):
    if random.random() > TYPE_DRIFT_PROB:
        return v
    # convert to string or list-like weirdness
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        try:
            num = float(''.join([c for c in v if c.isdigit() or c == '.']))
            return num if random.random() < 0.5 else v
        except Exception:
            return v
    return v

def maybe_duplicate_rows(row):
    if random.random() < DUPLICATE_ROW_PROB:
        rows = [row.copy(), row.copy()]
        # maybe near-duplicate tweak (typo/value change)
        if random.random() < NEAR_DUPLICATE_EDIT_PROB:
            k = random.choice(list(row.keys()))
            if isinstance(rows[1][k], str) and rows[1][k]:
                rows[1][k] = rows[1][k] + random.choice([" ", "  ", ".", "🙂"])
            elif isinstance(rows[1][k], (int, float)):
                rows[1][k] = rows[1][k] + random.choice([1, -1, 0])
        return rows
    return [row]

def maybe_schema_drift(row):
    if random.random() < SCHEMA_DRIFT_PROB:
        row = row.copy()
        if random.random() < 0.5:
            row["ExtraNote"] = random.choice(["legacy import", "migrated", ""])
        else:
            row["LegacyCode"] = random.randint(1000, 9999)
    return row

def format_date_chaos(dt):
    if isinstance(dt, (datetime,)):
        date_obj = dt
    else:
        date_obj = datetime.combine(dt, datetime.min.time())

    # Randomly vary format
    choice = random.random()
    if choice < 0.25:
        s = date_obj.strftime("%Y-%m-%d")
    elif choice < 0.5:
        s = date_obj.strftime("%d/%m/%Y")
    elif choice < 0.75:
        s = date_obj.isoformat() + ("Z" if random.random() < 0.5 else "")
    else:
        s = date_obj.strftime("%d-%b-%Y")
    return s

def maybe_future_date(dt):
    if random.random() < FUTURE_DATE_PROB:
        return dt + timedelta(days=random.randint(1, 365))
    return dt

# ---------------------------
# NHS number (Mod 11)
# ---------------------------
def generate_valid_nhs_number():
    digits = [random.randint(0, 9) for _ in range(9)]
    weights = list(range(10, 1, -1))  # 10..2
    total = sum(d * w for d, w in zip(digits, weights))
    remainder = total % 11
    check = 11 - remainder
    if check == 11:
        check = 0
    if check == 10:
        # regenerate to avoid invalid check digit
        return generate_valid_nhs_number()
    digits.append(check)
    s = "".join(str(d) for d in digits)
    # format 3-3-4 sometimes
    if random.random() < 0.8:
        return f"{s[0:3]} {s[3:6]} {s[6:10]}"
    return s

def generate_nhs_number():
    if random.random() < INVALID_NHS_PROB:
        # corrupted or wrong check
        base = [random.randint(0, 9) for _ in range(10)]
        s = "".join(map(str, base))
        if random.random() < 0.5:
            s = f"{s[0:3]} {s[3:6]} {s[6:10]}"
        return s
    return generate_valid_nhs_number()

# ---------------------------
# Domains
# ---------------------------
patients = []
encounters = []
labs = []
device_readings = []
registry = []
imaging = []
medications = []
procedures = []
claims = []

# Reference sets
lab_types = ["Potassium", "Sodium", "Hemoglobin", "WBC", "Glucose", "Creatinine", "CRP"]
device_types = ["HeartRate", "BloodPressure", "SpO2", "Temp"]
encounter_types = ["Inpatient", "Outpatient", "ER", "Virtual"]
insurance_types = ["Private", "Public", "None"]
modalities = ["CT", "MRI", "XRay", "Ultrasound"]
drug_names = ["Atorvastatin", "Metformin", "Lisinopril", "Ibuprofen", "Amoxicillin", "Levothyroxine"]
procedure_texts = ["Appendectomy", "MRI Scan", "Blood Transfusion", "Knee Surgery", "Cataract Surgery"]

# ---------------------------
# 1) Patients & Encounters
# ---------------------------
for i in range(1, NUM_PATIENTS + 1):
    patient_id = f"PAT{i:06d}"

    dob = random_past_date(days_back=365 * 95, min_back=365 * 1)
    gender = random.choices(["M", "F", "U", "Unknown"], weights=[0.45, 0.45, 0.05, 0.05])[0]
    nhs = generate_nhs_number()

    p = {
        "PatientID": patient_id,
        "NHSNumber": nhs,
        "FirstName": fake.first_name(),
        "LastName": fake.last_name(),
        "Gender": gender,
        "DOB": format_date_chaos(dob.date()),
        "Address": maybe_whitespace(fake.address().replace("\n", ", ")),
        "Phone": maybe_missing(fake.phone_number()),
        "Email": maybe_missing(fake.free_email()),
    }
    # Drift and types
    p = maybe_schema_drift(p)
    p = {k: maybe_type_drift(maybe_case(v)) for k, v in p.items()}
    for drow in maybe_duplicate_rows(p):
        patients.append(drow)

    # Encounters
    num_enc = random.randint(1, MAX_ENCOUNTERS)
    for e in range(num_enc):
        enc_id = f"ENC{i:06d}{e+1:02d}"
        enc_dt = maybe_future_date(random_past_date(days_back=365 * 6, min_back=0))

        enc = {
            "EncounterID": enc_id,
            "PatientID": patient_id,
            "EncounterDate": format_date_chaos(enc_dt),
            "EncounterType": random.choice(encounter_types),
            "Location": maybe_whitespace(fake.city()),
        }
        enc = maybe_schema_drift(enc)
        enc = {k: maybe_type_drift(maybe_case(maybe_missing(v))) for k, v in enc.items()}
        for drow in maybe_duplicate_rows(enc):
            encounters.append(drow)

# ---------------------------
# 2) Labs / Observations
# ---------------------------
for enc in encounters:
    if random.random() < 0.1:
        continue  # some encounters without labs
    n = random.randint(1, MAX_LABS_PER_ENC)
    for _ in range(n):
        lab_id = str(uuid.uuid4())
        lab_type = random.choice(lab_types)

        # Base value ranges with occasional outliers
        if lab_type == "Potassium":
            val = round(random.uniform(2.5, 6.5), 2)
        elif lab_type == "Sodium":
            val = round(random.uniform(120, 160), 1)
        elif lab_type == "Hemoglobin":
            val = round(random.uniform(6, 18), 1)
        elif lab_type == "WBC":
            val = round(random.uniform(2, 20), 1)
        elif lab_type == "Glucose":
            val = round(random.uniform(2.5, 22.0), 1)
        elif lab_type == "Creatinine":
            val = round(random.uniform(40, 300), 1)
        else:  # CRP
            val = round(random.uniform(0, 200), 1)

        if random.random() < OUTLIER_PROB:
            val = val * random.choice([0.1, 3, 5])

        # Mixed units or nonsense as strings
        if random.random() < TYPE_DRIFT_PROB:
            val = f"{val}{random.choice(['', ' mg/dL', ' mmol/L'])}"

        lrow = {
            "LabID": lab_id,
            "EncounterID": enc.get("EncounterID", ""),
            "PatientID": enc.get("PatientID", ""),
            "LabType": lab_type,
            "Value": maybe_missing(val),
            "LabDate": enc.get("EncounterDate", ""),
        }
        lrow = maybe_schema_drift(lrow)
        lrow = {k: maybe_type_drift(maybe_case(maybe_whitespace(v))) for k, v in lrow.items()}

        # Introduce bad references
        if random.random() < BAD_REF_PROB:
            lrow["EncounterID"] = f"ENC{random.randint(999999, 9999999)}"
            if random.random() < 0.5:
                lrow["PatientID"] = f"PAT{random.randint(999999, 9999999)}"

        for drow in maybe_duplicate_rows(lrow):
            labs.append(drow)

# ---------------------------
# 3) Device / IoMT readings
# ---------------------------
for p in patients:
    num = random.randint(1, MAX_DEVICE_READINGS_PER_PAT)
    for _ in range(num):
        reading_id = str(uuid.uuid4())
        device_type = random.choice(device_types)
        ts = random_recent_timestamp(days=30)

        if device_type == "HeartRate":
            value = random.randint(45, 140)
            if random.random() < OUTLIER_PROB:
                value = random.choice([0, 250])
        elif device_type == "BloodPressure":
            syst = random.randint(90, 160)
            dias = random.randint(55, 100)
            if random.random() < OUTLIER_PROB:
                syst, dias = dias, syst  # swapped
            value = f"{syst}/{dias}"
        elif device_type == "SpO2":
            value = random.randint(80, 100)
        else:  # Temp
            temp_c = round(random.uniform(34.0, 40.5), 1)
            if random.random() < TYPE_DRIFT_PROB:
                # Fahrenheit drift
                temp_c = round(temp_c * 9/5 + 32, 1)
            value = temp_c

        drow = {
            "ReadingID": reading_id,
            "PatientID": p.get("PatientID", ""),
            "DeviceType": device_type,
            "Value": maybe_missing(value),
            "Timestamp": ts if random.random() < 0.7 else format_date_chaos(datetime.now()),
        }
        drow = maybe_schema_drift(drow)
        drow = {k: maybe_type_drift(maybe_case(maybe_whitespace(v))) for k, v in drow.items()}
        if random.random() < BAD_REF_PROB:
            drow["PatientID"] = f"PAT{random.randint(999999, 9999999)}"
        for rr in maybe_duplicate_rows(drow):
            device_readings.append(rr)

# ---------------------------
# 4) Registry / Administrative CSV
# ---------------------------
for p in patients:
    r = {
        "PatientID": p.get("PatientID", ""),
        "InsuranceType": random.choice(insurance_types),
        "Region": maybe_whitespace(fake.county()),
        "EnrollmentDate": format_date_chaos(random_past_date(days_back=365 * 15, min_back=365).date()),
    }
    r = maybe_schema_drift(r)
    r = {k: maybe_type_drift(maybe_case(maybe_missing(v))) for k, v in r.items()}
    if random.random() < BAD_REF_PROB:
        r["PatientID"] = f"PAT{random.randint(999999, 9999999)}"
    for rr in maybe_duplicate_rows(r):
        registry.append(rr)

# ---------------------------
# 5) Imaging metadata
# ---------------------------
for enc in encounters:
    nimg = random.randint(0, MAX_IMAGING_PER_ENC)
    for _ in range(nimg):
        image_id = str(uuid.uuid4())
        modality = random.choice(modalities)
        imaging_date = enc.get("EncounterDate", format_date_chaos(datetime.now()))

        im = {
            "ImageID": image_id,
            "EncounterID": enc.get("EncounterID", ""),
            "PatientID": enc.get("PatientID", ""),
            "Modality": modality,
            "ImagingDate": imaging_date,
        }
        im = maybe_schema_drift(im)
        im = {k: maybe_type_drift(maybe_case(maybe_missing(v))) for k, v in im.items()}
        if random.random() < BAD_REF_PROB:
            im["EncounterID"] = f"ENC{random.randint(999999, 9999999)}"
        for rr in maybe_duplicate_rows(im):
            imaging.append(rr)

# ---------------------------
# 6) Medications (CSV)
# ---------------------------
for p in patients:
    count = random.randint(*MEDS_PER_PAT_RANGE)
    for _ in range(count):
        start = random_past_date(days_back=365 * 5, min_back=0)
        end = start + timedelta(days=random.randint(1, 400))
        m = {
            "PatientID": p.get("PatientID", ""),
            "EncounterID": random.choice(encounters)["EncounterID"] if encounters and random.random() > BAD_REF_PROB else f"ENC{random.randint(500000,999999)}",
            "DrugName": random.choice(drug_names),
            "Dosage": f"{random.randint(1, 500)}mg",
            "Route": random.choice(["Oral", "IV", "Subcutaneous", "Topical"]),
            "StartDate": format_date_chaos(start.date()),
            "EndDate": format_date_chaos(maybe_future_date(end).date()),
        }
        m = maybe_schema_drift(m)
        m = {k: maybe_type_drift(maybe_case(maybe_missing(v))) for k, v in m.items()}
        for rr in maybe_duplicate_rows(m):
            medications.append(rr)

# ---------------------------
# 7) Procedures (FHIR-like JSON)
# ---------------------------
for p in patients:
    count = random.randint(*PROCS_PER_PAT_RANGE)
    for _ in range(count):
        procedures.append(
            {
                "resourceType": "Procedure",
                "id": str(uuid.uuid4()),
                "subject": {"reference": f"Patient/{p.get('PatientID','')}"},
                "encounter": {"reference": f"Encounter/{random.choice(encounters)['EncounterID']}" if encounters and random.random() > BAD_REF_PROB else f"Encounter/ENC{random.randint(500000,999999)}"},
                "code": {"text": random.choice(procedure_texts)},
                "performedDateTime": random_recent_timestamp(days=900) if random.random() < 0.5 else format_date_chaos(datetime.now()),
                "status": random.choice(["completed", "in-progress", "scheduled", "entered-in-error"]),
            }
        )

# ---------------------------
# 8) Claims (CSV)
# ---------------------------
for p in patients:
    count = random.randint(*CLAIMS_PER_PAT_RANGE)
    for _ in range(count):
        claims.append(
            {
                "ClaimID": str(uuid.uuid4()),
                "PatientID": p.get("PatientID", ""),
                "EncounterID": random.choice(encounters)["EncounterID"] if encounters and random.random() > BAD_REF_PROB else f"ENC{random.randint(500000,999999)}",
                "ServiceDate": format_date_chaos(random_past_date(days_back=365 * 5, min_back=0).date()),
                "ClaimAmount": round(random.uniform(50, 15000), 2),
                "Status": random.choice(["Pending", "Paid", "Denied", "Reversed"]),
            }
        )

# ---------------------------
# FHIR-like bundles for core resources
# ---------------------------
fhir_patients = [
    {
        "resourceType": "Patient",
        "id": p["PatientID"],
        "identifier": [{"system": "https://fhir.nhs.uk/Id/nhs-number", "value": p.get("NHSNumber")}],
        "name": [{"given": [p.get("FirstName")], "family": p.get("LastName")}],
        "gender": (p.get("Gender") or "").lower() if isinstance(p.get("Gender"), str) else p.get("Gender"),
        "birthDate": p.get("DOB"),
        "address": [{"text": p.get("Address")}],
        "telecom": [{"system": "phone", "value": p.get("Phone")}, {"system": "email", "value": p.get("Email")}],
    }
    for p in patients
]

fhir_encounters = [
    {
        "resourceType": "Encounter",
        "id": e.get("EncounterID"),
        "subject": {"reference": f"Patient/{e.get('PatientID')}"},
        "period": {"start": e.get("EncounterDate")},
        "class": {"code": e.get("EncounterType")},
        "serviceProvider": {"display": e.get("Location")},
    }
    for e in encounters
]

fhir_observations = []
for l in labs:
    fhir_observations.append(
        {
            "resourceType": "Observation",
            "id": l.get("LabID"),
            "subject": {"reference": f"Patient/{l.get('PatientID')}"},
            "encounter": {"reference": f"Encounter/{l.get('EncounterID')}"},
            "code": {"text": l.get("LabType")},
            "valueString": str(l.get("Value")),
            "effectiveDateTime": l.get("LabDate"),
            "status": random.choice(["final", "amended", "corrected"]),
        }
    )

# ---------------------------
# Export helpers
# ---------------------------
def write_csv(filename, rows, fieldnames):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Wrote {filename} ({len(rows)} rows)")

def write_json(filename, data):
    def convert(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(item) for item in obj]
        else:
            return obj

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(convert(data), f, indent=2, ensure_ascii=False)
    print(f"Wrote {filename} with {len(data)} records.")

def write_jsonl(filename, rows):
    def convert(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(item) for item in obj]
        else:
            return obj

    with open(filename, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(convert(r), ensure_ascii=False) + "\n")
    print(f"Wrote {filename} ({len(rows)} docs)")

# ---------------------------
# Export CSVs
# ---------------------------
write_csv("patients.csv", patients, ["PatientID","NHSNumber","FirstName","LastName","Gender","DOB","Address","Phone","Email"])
write_csv("encounters.csv", encounters, ["EncounterID","PatientID","EncounterDate","EncounterType","Location"])
write_csv("labs.csv", labs, ["LabID","EncounterID","PatientID","LabType","Value","LabDate"])
write_csv("device_readings.csv", device_readings, ["ReadingID","PatientID","DeviceType","Value","Timestamp"])
write_csv("registry.csv", registry, ["PatientID","InsuranceType","Region","EnrollmentDate"])
write_csv("imaging.csv", imaging, ["ImageID","EncounterID","PatientID","Modality","ImagingDate"])
write_csv("medications.csv", medications, ["PatientID","EncounterID","DrugName","Dosage","Route","StartDate","EndDate"])
write_csv("claims.csv", claims, ["ClaimID","PatientID","EncounterID","ServiceDate","ClaimAmount","Status"])

# ---------------------------
# Export JSONL (per collection + Mongo-ready)
# ---------------------------
write_jsonl("patients.jsonl", patients)
write_jsonl("encounters.jsonl", encounters)
write_jsonl("labs.jsonl", labs)
write_jsonl("device_readings.jsonl", device_readings)
write_jsonl("registry.jsonl", registry)
write_jsonl("imaging.jsonl", imaging)

write_jsonl("mongo_patients.jsonl", [{"_id": p["PatientID"], **p} for p in patients])
write_jsonl("mongo_encounters.jsonl", [{"_id": e["EncounterID"], **e} for e in encounters])
write_jsonl("mongo_labs.jsonl", labs)
write_jsonl("mongo_device_readings.jsonl", device_readings)

# ---------------------------
# Export FHIR-like JSON
# ---------------------------
write_json("procedures.json", procedures)
write_json("fhir_patients.json", fhir_patients)
write_json("fhir_encounters.json", fhir_encounters)
write_json("fhir_observations.json", fhir_observations)

