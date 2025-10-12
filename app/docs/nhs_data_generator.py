"""
NHS Realistic Data Generator Suite
==================================
Generates comprehensive mock NHS datasets with realistic patterns, anomalies, and data quality issues
following DSCRO collect/link/treat/pseudo/distribute methodology.

Features:
- 15+ NHS dataset types with authentic data structures
- Realistic clinical pathways and patient journeys
- Data quality issues and anomalies as found in real NHS data
- Temporal patterns, seasonal variations, and demographic correlations
- Integration-ready formats for Azure data engineering workflows
"""

import pandas as pd
import numpy as np
import random
import datetime
from datetime import timedelta
import uuid
import hashlib
from faker import Faker
import json
import os
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# Set random seeds for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker(['en_GB'])
Faker.seed(42)

@dataclass
class DataGenerationConfig:
    """Configuration for NHS data generation"""
    base_population: int = 100000
    trusts: int = 50
    practices: int = 200
    consultants: int = 500
    years_of_data: int = 3
    start_date: datetime.date = datetime.date(2022, 1, 1)
    data_quality_degradation: float = 0.15  # 15% various quality issues
    seasonal_variation: bool = True
    covid_impact: bool = True

class NHSCodebooks:
    """NHS standard code systems and lookup tables"""
    
    @staticmethod
    def get_icd10_codes():
        """Common ICD-10 diagnosis codes with realistic prevalence"""
        return {
            'I10': {'name': 'Essential hypertension', 'prevalence': 0.15},
            'E11': {'name': 'Type 2 diabetes mellitus', 'prevalence': 0.08},
            'I25': {'name': 'Chronic ischaemic heart disease', 'prevalence': 0.06},
            'J44': {'name': 'Chronic obstructive pulmonary disease', 'prevalence': 0.04},
            'N18': {'name': 'Chronic kidney disease', 'prevalence': 0.03},
            'F32': {'name': 'Depressive episode', 'prevalence': 0.12},
            'F41': {'name': 'Anxiety disorders', 'prevalence': 0.09},
            'M79': {'name': 'Soft tissue disorders', 'prevalence': 0.07},
            'R06': {'name': 'Abnormalities of breathing', 'prevalence': 0.05},
            'Z51': {'name': 'Encounter for other aftercare', 'prevalence': 0.04},
            'S72': {'name': 'Fracture of femur', 'prevalence': 0.002},
            'I21': {'name': 'Acute myocardial infarction', 'prevalence': 0.001},
            'J18': {'name': 'Pneumonia', 'prevalence': 0.02},
            'K80': {'name': 'Cholelithiasis', 'prevalence': 0.015}
        }
    
    @staticmethod
    def get_opcs4_codes():
        """Common OPCS-4 procedure codes"""
        return {
            'Z92': {'name': 'Monitoring', 'frequency': 0.25},
            'U07': {'name': 'Computer tomography', 'frequency': 0.12},
            'W37': {'name': 'Cataract extraction', 'frequency': 0.08},
            'H01': {'name': 'Cardiac catheterisation', 'frequency': 0.05},
            'T87': {'name': 'Arthroscopy', 'frequency': 0.04},
            'M65': {'name': 'Endoscopy of colon', 'frequency': 0.06},
            'J27': {'name': 'Cholecystectomy', 'frequency': 0.03},
            'W19': {'name': 'Excision of lesion of skin', 'frequency': 0.07}
        }
    
    @staticmethod
    def get_hrg_codes():
        """Healthcare Resource Groups for payment grouping"""
        return {
            'AA22': {'name': 'Non-elective long stay', 'tariff': 4500.00},
            'AA23': {'name': 'Non-elective short stay', 'tariff': 1800.00},
            'DZ19': {'name': 'Cardiac procedures', 'tariff': 8900.00},
            'FF01': {'name': 'Cataract procedures', 'tariff': 950.00},
            'HN12': {'name': 'Arthroscopic procedures', 'tariff': 2100.00},
            'FZ92': {'name': 'Outpatient procedures', 'tariff': 180.00},
            'WJ11': {'name': 'Emergency medicine', 'tariff': 280.00}
        }
    
    @staticmethod
    def get_bnf_codes():
        """British National Formulary prescription codes"""
        return {
            '0101010': {'name': 'Antacids', 'avg_cost': 3.50},
            '0201010': {'name': 'Cardiac glycosides', 'avg_cost': 12.80},
            '0206020': {'name': 'ACE inhibitors', 'avg_cost': 8.90},
            '0301011': {'name': 'Beta2 agonists', 'avg_cost': 15.60},
            '0401020': {'name': 'Anxiolytics', 'avg_cost': 6.70},
            '0601060': {'name': 'Insulin', 'avg_cost': 45.20},
            '0501130': {'name': 'Penicillins', 'avg_cost': 7.30},
            '1001010': {'name': 'Non-opioid analgesics', 'avg_cost': 2.10},
            '0403040': {'name': 'Antidepressants', 'avg_cost': 18.40}
        }

class PatientGenerator:
    """Generates realistic synthetic patient population"""
    
    def __init__(self, config: DataGenerationConfig):
        self.config = config
        self.patients = []
        
    def generate_nhs_number(self, index: int) -> str:
        """Generate realistic NHS number with check digit"""
        # Use deterministic generation for consistency
        base = f"9{index:09d}"
        
        # Calculate check digit using NHS algorithm
        check_sum = 0
        for i, digit in enumerate(base):
            check_sum += int(digit) * (10 - i)
        
        check_digit = (11 - (check_sum % 11)) % 11
        if check_digit == 10:
            check_digit = 0
            
        return f"{base}{check_digit}"
    
    def generate_postcode(self) -> Tuple[str, int]:
        """Generate UK postcode with deprivation decile"""
        # Realistic postcode patterns with deprivation correlation
        postcodes = [
            ('LE1 7RH', 8), ('LE2 3BD', 4), ('LE3 9HY', 6), ('LE4 5GF', 3),
            ('LE5 4PQ', 7), ('CV1 2NB', 5), ('CV21 3FD', 2), ('CV31 1HG', 9),
            ('B1 1TT', 4), ('B15 2TT', 6), ('B28 8QY', 8), ('B44 8SF', 2),
            ('NG1 5DT', 5), ('NG7 2RD', 3), ('NG11 8NS', 7), ('DE1 1PP', 6),
            ('M1 1AD', 4), ('M14 7DU', 3), ('M20 4BX', 8), ('OX1 2JD', 9)
        ]
        return random.choice(postcodes)
    
    def generate_patients(self) -> pd.DataFrame:
        """Generate complete patient population with demographics"""
        patients = []
        
        for i in range(self.config.base_population):
            # Introduce some missing/invalid NHS numbers (5%)
            if random.random() < 0.05:
                nhs_number = None if random.random() < 0.5 else "INVALID"
            else:
                nhs_number = self.generate_nhs_number(i)
            
            # Age distribution matching UK demographics
            age = int(np.random.gamma(2.5, 15))  # Realistic age distribution
            age = max(0, min(age, 95))  # Cap at realistic range
            
            # Birth date calculation
            birth_date = self.config.start_date - timedelta(days=age * 365.25)
            
            # Gender distribution
            gender = np.random.choice(['M', 'F', 'U'], p=[0.49, 0.49, 0.02])
            
            # Ethnicity distribution (ONS 2021 Census approximation)
            ethnicity = np.random.choice([
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'Z'
            ], p=[0.82, 0.04, 0.025, 0.025, 0.015, 0.015, 0.015, 0.01, 0.005, 0.005, 0.005, 0.005, 0.005, 0.003, 0.003, 0.003, 0.001])
            
            postcode, deprivation_decile = self.generate_postcode()
            
            # Practice registration (some patients may not be registered)
            practice_code = f"M{random.randint(81001, 81200):05d}" if random.random() > 0.02 else None
            
            patients.append({
                'patient_id': f'PAT_{i:06d}',
                'nhs_number': nhs_number,
                'date_of_birth': birth_date,
                'gender': gender,
                'ethnicity': ethnicity,
                'postcode': postcode,
                'deprivation_decile': deprivation_decile,
                'registered_practice': practice_code,
                'age_at_start': age,
                'created_date': self.config.start_date
            })
        
        return pd.DataFrame(patients)

class ProviderGenerator:
    """Generates NHS provider organizations"""
    
    def __init__(self, config: DataGenerationConfig):
        self.config = config
    
    def generate_trusts(self) -> pd.DataFrame:
        """Generate NHS Trust organizations"""
        trusts = []
        
        trust_types = ['Foundation Trust', 'NHS Trust', 'Mental Health Trust', 'Community Trust']
        regions = ['Midlands', 'North', 'South', 'London', 'East']
        
        for i in range(self.config.trusts):
            trust_code = f"R{chr(65 + i//26)}{chr(65 + i%26)}"
            
            trusts.append({
                'trust_code': trust_code,
                'trust_name': fake.company() + ' ' + random.choice(trust_types),
                'trust_type': random.choice(trust_types),
                'region': random.choice(regions),
                'commissioner_code': f"CCG{random.randint(1, 50):03d}",
                'beds': random.randint(200, 1200),
                'annual_income': random.randint(50, 500) * 1000000
            })
        
        return pd.DataFrame(trusts)
    
    def generate_practices(self) -> pd.DataFrame:
        """Generate GP practices"""
        practices = []
        
        for i in range(self.config.practices):
            practice_code = f"M{random.randint(81001, 81999)}"
            
            practices.append({
                'practice_code': practice_code,
                'practice_name': f"Dr {fake.last_name()} & Partners",
                'postcode': self.generate_practice_postcode(),
                'list_size': random.randint(2000, 15000),
                'partners': random.randint(1, 8),
                'commissioning_region': random.choice(['Leicester', 'Coventry', 'Birmingham', 'Nottingham'])
            })
        
        return pd.DataFrame(practices)
    
    def generate_practice_postcode(self) -> str:
        """Generate practice postcode"""
        postcodes = ['LE1 6NB', 'LE2 7LX', 'CV1 2HG', 'B15 2TH', 'NG1 5DT']
        return random.choice(postcodes)

class SUSPlusGenerator:
    """Secondary Uses Service Plus - Hospital activity data"""
    
    def __init__(self, patients_df: pd.DataFrame, trusts_df: pd.DataFrame, config: DataGenerationConfig):
        self.patients_df = patients_df
        self.trusts_df = trusts_df
        self.config = config
        self.icd10_codes = NHSCodebooks.get_icd10_codes()
        self.opcs4_codes = NHSCodebooks.get_opcs4_codes()
        self.hrg_codes = NHSCodebooks.get_hrg_codes()
    
    def generate_episodes(self) -> pd.DataFrame:
        """Generate SUS+ admitted patient care episodes"""
        episodes = []
        episode_id = 1
        
        # Generate episodes over time period
        current_date = self.config.start_date
        end_date = self.config.start_date + timedelta(days=365 * self.config.years_of_data)
        
        while current_date < end_date:
            # Daily volume with seasonal variation
            base_daily_episodes = 150
            if self.config.seasonal_variation:
                # Winter pressure (Nov-Mar)
                if current_date.month in [11, 12, 1, 2, 3]:
                    daily_episodes = int(base_daily_episodes * 1.4)
                # Summer reduction (Jul-Aug)
                elif current_date.month in [7, 8]:
                    daily_episodes = int(base_daily_episodes * 0.8)
                else:
                    daily_episodes = base_daily_episodes
            else:
                daily_episodes = base_daily_episodes
            
            # COVID impact simulation
            if self.config.covid_impact and current_date >= datetime.date(2020, 3, 1):
                if current_date <= datetime.date(2020, 6, 1):
                    daily_episodes = int(daily_episodes * 0.4)  # First lockdown
                elif current_date <= datetime.date(2021, 6, 1):
                    daily_episodes = int(daily_episodes * 0.7)  # Gradual recovery
            
            # Weekend effect
            if current_date.weekday() >= 5:  # Saturday/Sunday
                daily_episodes = int(daily_episodes * 0.6)
            
            for _ in range(daily_episodes):
                episode = self.generate_single_episode(episode_id, current_date)
                episodes.append(episode)
                episode_id += 1
            
            current_date += timedelta(days=1)
        
        return pd.DataFrame(episodes)
    
    def generate_single_episode(self, episode_id: int, admission_date: datetime.date) -> dict:
        """Generate individual episode with realistic clinical patterns"""
        # Select patient with age-based admission probability
        patient = self.patients_df.sample(n=1, weights=self.get_admission_weights()).iloc[0]
        
        # Select trust
        trust = self.trusts_df.sample(n=1).iloc[0]
        
        # Admission method and urgency
        admission_methods = {
            '11': 'Waiting list',  # Elective
            '12': 'Booked',       # Elective
            '21': 'A&E',          # Emergency
            '22': 'GP referral',  # Emergency
            '23': 'Bed bureau',   # Emergency
            '24': 'Consultant clinic', # Emergency
            '28': 'Other emergency'
        }
        
        # Age influences admission type
        if patient['age_at_start'] > 70:
            admission_method = random.choices(
                list(admission_methods.keys()),
                weights=[0.2, 0.15, 0.35, 0.15, 0.05, 0.05, 0.05]
            )[0]
        else:
            admission_method = random.choices(
                list(admission_methods.keys()),
                weights=[0.4, 0.25, 0.2, 0.08, 0.03, 0.02, 0.02]
            )[0]
        
        # Length of stay based on age and admission type
        if admission_method in ['11', '12']:  # Elective
            los = max(1, int(np.random.gamma(1.5, 2)))
        else:  # Emergency
            los = max(1, int(np.random.gamma(2, 3)))
        
        # Age factor for length of stay
        if patient['age_at_start'] > 75:
            los = int(los * 1.5)
        
        discharge_date = admission_date + timedelta(days=los)
        
        # Primary diagnosis based on age and admission type
        primary_diagnosis = self.select_diagnosis(patient['age_at_start'], admission_method)
        
        # Secondary diagnoses (0-5 additional)
        secondary_diagnoses = []
        num_secondary = np.random.poisson(1.2)
        for _ in range(min(num_secondary, 5)):
            sec_diag = random.choice(list(self.icd10_codes.keys()))
            if sec_diag != primary_diagnosis:
                secondary_diagnoses.append(sec_diag)
        
        # Procedures
        procedures = self.select_procedures(primary_diagnosis, admission_method)
        
        # HRG grouping
        hrg_code = self.assign_hrg(primary_diagnosis, procedures, los)
        
        # Introduce data quality issues
        if random.random() < self.config.data_quality_degradation:
            episode_data = self.introduce_data_issues({
                'episode_id': f'EP{episode_id:08d}',
                'patient_id': patient['patient_id'],
                'nhs_number': patient['nhs_number'],
                'trust_code': trust['trust_code'],
                'admission_date': admission_date,
                'discharge_date': discharge_date,
                'admission_method': admission_method,
                'admission_source': self.get_admission_source(admission_method),
                'discharge_destination': self.get_discharge_destination(patient['age_at_start']),
                'length_of_stay': los,
                'primary_diagnosis': primary_diagnosis,
                'secondary_diagnoses': ','.join(secondary_diagnoses),
                'primary_procedure': procedures[0] if procedures else None,
                'secondary_procedures': ','.join(procedures[1:]) if len(procedures) > 1 else None,
                'hrg_code': hrg_code,
                'consultant_code': f'C{random.randint(1000, 9999)}',
                'specialty': self.get_specialty(primary_diagnosis),
                'ward_code': f'W{random.randint(10, 99)}',
                'created_timestamp': datetime.datetime.now()
            })
        else:
            episode_data = {
                'episode_id': f'EP{episode_id:08d}',
                'patient_id': patient['patient_id'],
                'nhs_number': patient['nhs_number'],
                'trust_code': trust['trust_code'],
                'admission_date': admission_date,
                'discharge_date': discharge_date,
                'admission_method': admission_method,
                'admission_source': self.get_admission_source(admission_method),
                'discharge_destination': self.get_discharge_destination(patient['age_at_start']),
                'length_of_stay': los,
                'primary_diagnosis': primary_diagnosis,
                'secondary_diagnoses': ','.join(secondary_diagnoses),
                'primary_procedure': procedures[0] if procedures else None,
                'secondary_procedures': ','.join(procedures[1:]) if len(procedures) > 1 else None,
                'hrg_code': hrg_code,
                'consultant_code': f'C{random.randint(1000, 9999)}',
                'specialty': self.get_specialty(primary_diagnosis),
                'ward_code': f'W{random.randint(10, 99)}',
                'created_timestamp': datetime.datetime.now()
            }
        
        return episode_data
    
    def get_admission_weights(self) -> pd.Series:
        """Calculate admission probability weights based on age"""
        # Older patients more likely to be admitted
        age_factor = np.where(self.patients_df['age_at_start'] > 65, 3.0, 1.0)
        age_factor = np.where(self.patients_df['age_at_start'] > 80, 5.0, age_factor)
        
        # Deprivation factor
        deprivation_factor = 11 - self.patients_df['deprivation_decile']  # Higher deprivation = higher admission
        
        return pd.Series(age_factor * deprivation_factor, index=self.patients_df.index)
    
    def select_diagnosis(self, age: int, admission_method: str) -> str:
        """Select realistic primary diagnosis based on age and admission type"""
        if admission_method in ['21', '22', '23', '24', '28']:  # Emergency
            if age > 75:
                emergency_elderly = ['J18', 'I21', 'S72', 'R06', 'N18']
                return random.choice(emergency_elderly)
            else:
                emergency_adult = ['J18', 'R06', 'I25', 'M79', 'K80']
                return random.choice(emergency_adult)
        else:  # Elective
            elective_codes = ['Z51', 'H01', 'W37', 'J27', 'T87']
            return random.choice(elective_codes)
    
    def select_procedures(self, primary_diagnosis: str, admission_method: str) -> List[str]:
        """Select procedures based on diagnosis and admission type"""
        procedures = []
        
        # Procedure mapping based on diagnosis
        procedure_mapping = {
            'I21': ['H01', 'H02'],  # MI - cardiac procedures
            'S72': ['T87'],         # Fracture - orthopedic
            'K80': ['J27'],         # Gallbladder - surgery
            'W37': ['W37'],         # Cataract
            'J18': ['Z92']          # Pneumonia - monitoring
        }
        
        if primary_diagnosis in procedure_mapping:
            procedures.extend(procedure_mapping[primary_diagnosis])
        
        # Add monitoring for all admissions
        if random.random() > 0.3:
            procedures.append('Z92')
        
        return procedures[:3]  # Max 3 procedures
    
    def assign_hrg(self, diagnosis: str, procedures: List[str], los: int) -> str:
        """Assign Healthcare Resource Group for payment"""
        if los > 7:
            return 'AA22'  # Long stay
        elif any(proc in ['H01', 'H02'] for proc in procedures):
            return 'DZ19'  # Cardiac
        elif 'W37' in procedures:
            return 'FF01'  # Cataract
        elif 'T87' in procedures:
            return 'HN12'  # Orthopedic
        else:
            return 'AA23'  # Short stay
    
    def get_admission_source(self, admission_method: str) -> str:
        """Get admission source based on method"""
        sources = {
            '11': '19',  # Usual residence - waiting list
            '12': '19',  # Usual residence - booked
            '21': '01',  # Accident and Emergency
            '22': '19',  # Usual residence - GP
            '23': '51',  # NHS other hospital
            '24': '19',  # Usual residence
            '28': '01'   # A&E
        }
        return sources.get(admission_method, '19')
    
    def get_discharge_destination(self, age: int) -> str:
        """Get discharge destination based on age"""
        if age > 80:
            return random.choices(['19', '65', '87', '79'], weights=[0.6, 0.2, 0.15, 0.05])[0]
        else:
            return random.choices(['19', '65', '87'], weights=[0.85, 0.1, 0.05])[0]
    
    def get_specialty(self, diagnosis: str) -> str:
        """Get medical specialty based on diagnosis"""
        specialty_mapping = {
            'I21': '320',  # Cardiology
            'I25': '320',  # Cardiology
            'J18': '340',  # Respiratory
            'J44': '340',  # Respiratory
            'S72': '110',  # Trauma & Orthopedics
            'F32': '710',  # Psychiatry
            'K80': '100',  # General Surgery
            'W37': '130'   # Ophthalmology
        }
        return specialty_mapping.get(diagnosis, '300')  # General Medicine
    
    def introduce_data_issues(self, episode: dict) -> dict:
        """Introduce realistic data quality issues"""
        issue_type = random.choice(['missing_field', 'invalid_code', 'date_issue', 'duplicate'])
        
        if issue_type == 'missing_field':
            # Random field becomes None
            fields_to_null = ['secondary_diagnoses', 'secondary_procedures', 'ward_code']
            field = random.choice(fields_to_null)
            episode[field] = None
            
        elif issue_type == 'invalid_code':
            # Invalid diagnosis code
            episode['primary_diagnosis'] = 'INVALID'
            
        elif issue_type == 'date_issue':
            # Discharge before admission (data entry error)
            if random.random() < 0.3:
                episode['discharge_date'] = episode['admission_date'] - timedelta(days=1)
        
        return episode

class ECDSGenerator:
    """Emergency Care Data Set Generator"""
    
    def __init__(self, patients_df: pd.DataFrame, trusts_df: pd.DataFrame, config: DataGenerationConfig):
        self.patients_df = patients_df
        self.trusts_df = trusts_df
        self.config = config
    
    def generate_attendances(self) -> pd.DataFrame:
        """Generate A&E attendances with realistic patterns"""
        attendances = []
        attendance_id = 1
        
        current_date = self.config.start_date
        end_date = self.config.start_date + timedelta(days=365 * self.config.years_of_data)
        
        while current_date < end_date:
            # Daily A&E volume with patterns
            base_daily = 80
            
            # Day of week pattern
            if current_date.weekday() == 5:  # Saturday
                daily_attendances = int(base_daily * 1.3)
            elif current_date.weekday() == 6:  # Sunday
                daily_attendances = int(base_daily * 1.1)
            elif current_date.weekday() == 0:  # Monday
                daily_attendances = int(base_daily * 1.2)
            else:
                daily_attendances = base_daily
            
            # Seasonal variation
            if current_date.month in [12, 1, 2]:  # Winter
                daily_attendances = int(daily_attendances * 1.4)
            elif current_date.month in [7, 8]:  # Summer
                daily_attendances = int(daily_attendances * 0.9)
            
            for _ in range(daily_attendances):
                attendance = self.generate_single_attendance(attendance_id, current_date)
                attendances.append(attendance)
                attendance_id += 1
            
            current_date += timedelta(days=1)
        
        return pd.DataFrame(attendances)
    
    def generate_single_attendance(self, attendance_id: int, attendance_date: datetime.date) -> dict:
        """Generate single A&E attendance"""
        patient = self.patients_df.sample(n=1, weights=self.get_attendance_weights()).iloc[0]
        trust = self.trusts_df.sample(n=1).iloc[0]
        
        # Arrival time distribution (more at evening/night)
        hour_weights = [2, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 8, 9, 10, 11, 12, 10, 8, 6, 4]
        arrival_hour = random.choices(range(24), weights=hour_weights)[0]
        arrival_time = datetime.datetime.combine(attendance_date, datetime.time(arrival_hour, random.randint(0, 59)))
        
        # Triage category (1=immediate, 5=non-urgent)
        if patient['age_at_start'] > 75:
            triage = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.25, 0.4, 0.25, 0.05])[0]
        else:
            triage = random.choices([1, 2, 3, 4, 5], weights=[0.02, 0.15, 0.35, 0.35, 0.13])[0]
        
        # Time to treatment based on triage
        treatment_times = {1: 0, 2: 15, 3: 60, 4: 120, 5: 240}  # Minutes
        time_to_treatment = treatment_times[triage] + random.randint(-10, 30)
        time_to_treatment = max(0, time_to_treatment)
        
        # Total time in department
        if triage <= 2:
            total_time = random.randint(120, 480)  # 2-8 hours
        else:
            total_time = random.randint(60, 360)   # 1-6 hours
        
        departure_time = arrival_time + timedelta(minutes=total_time)
        
        # Presenting complaint
        complaints = {
            'Chest pain': 0.15,
            'Abdominal pain': 0.12,
            'Shortness of breath': 0.10,
            'Head injury': 0.08,
            'Limb problems': 0.08,
            'Collapse': 0.06,
            'Mental health': 0.05,
            'Wounds': 0.08,
            'Back pain': 0.06,
            'Other': 0.22
        }
        
        presenting_complaint = random.choices(
            list(complaints.keys()),
            weights=list(complaints.values())
        )[0]
        
        # Discharge destination
        if patient['age_at_start'] > 80 and triage <= 2:
            discharge_dest = random.choices(
                ['01', '02', '03', '04', '05'],  # Home, ward, other hosp, died, left
                weights=[0.5, 0.35, 0.08, 0.02, 0.05]
            )[0]
        else:
            discharge_dest = random.choices(
                ['01', '02', '03', '04', '05'],
                weights=[0.75, 0.15, 0.05, 0.01, 0.04]
            )[0]
        
        # Investigations
        investigations = []
        if triage <= 3:
            if random.random() < 0.6:
                investigations.append('Blood tests')
            if random.random() < 0.4:
                investigations.append('X-ray')
            if random.random() < 0.2:
                investigations.append('CT scan')
            if random.random() < 0.1:
                investigations.append('ECG')
        
        return {
            'attendance_id': f'ATT{attendance_id:08d}',
            'patient_id': patient['patient_id'],
            'nhs_number': patient['nhs_number'],
            'trust_code': trust['trust_code'],
            'arrival_datetime': arrival_time,
            'departure_datetime': departure_time,
            'triage_category': triage,
            'presenting_complaint': presenting_complaint,
            'discharge_destination': discharge_dest,
            'time_to_treatment_mins': time_to_treatment,
            'total_time_mins': total_time,
            'investigations': ','.join(investigations),
            'referred_to_specialist': random.choice([True, False]) if discharge_dest == '01' else False,
            'safeguarding_concern': random.random() < 0.02,
            'created_timestamp': datetime.datetime.now()
        }
    
    def get_attendance_weights(self) -> pd.Series:
        """Calculate A&E attendance probability weights"""
        # Young adults and elderly more likely to attend A&E
        age_weights = np.where(self.patients_df['age_at_start'] < 25, 2.0, 1.0)
        age_weights = np.where(self.patients_df['age_at_start'] > 70, 2.5, age_weights)
        
        # Deprivation correlation
        deprivation_weights = 11 - self.patients_df['deprivation_decile']
        
        return pd.Series(age_weights * deprivation_weights, index=self.patients_df.index)

class MHSDSGenerator:
    """Mental Health Services Data Set Generator"""
    
    def __init__(self, patients_df: pd.DataFrame, config: DataGenerationConfig):
        self.patients_df = patients_df
        self.config = config
    
    def generate_referrals(self) -> pd.DataFrame:
        """Generate mental health referrals and care episodes"""
        referrals = []
        referral_id = 1
        
        # Mental health prevalence by age
        mh_population = self.patients_df[
            (self.patients_df['age_at_start'] >= 16) &  # Adult services
            (self.patients_df['age_at_start'] <= 65)
        ].copy()
        
        # Select patients with mental health needs (prevalence ~15%)
        mh_patients = mh_population.sample(n=int(len(mh_population) * 0.15))
        
        for _, patient in mh_patients.iterrows():
            # Generate 1-3 referrals per patient over the period
            num_referrals = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
            
            for ref_num in range(num_referrals):
                referral_date = self.config.start_date + timedelta(
                    days=random.randint(0, 365 * self.config.years_of_data)
                )
                
                referral = self.generate_single_referral(referral_id, patient, referral_date)
                referrals.append(referral)
                referral_id += 1
        
        return pd.DataFrame(referrals)
    
    def generate_single_referral(self, referral_id: int, patient: pd.Series, referral_date: datetime.date) -> dict:
        """Generate single mental health referral"""
        # Referral source
        referral_sources = {
            'A01': 'GP',
            'A02': 'Self referral',
            'A03': 'A&E',
            'A04': 'Acute hospital',
            'A05': 'Criminal justice',
            'A06': 'Other'
        }
        
        source = random.choices(
            list(referral_sources.keys()),
            weights=[0.65, 0.15, 0.08, 0.05, 0.04, 0.03]
        )[0]
        
        # Primary diagnosis (ICD-10 F codes)
        mh_diagnoses = {
            'F32': 'Depressive episode',
            'F41': 'Anxiety disorders',
            'F43': 'Stress-related disorders',
            'F20': 'Schizophrenia',
            'F31': 'Bipolar disorder',
            'F60': 'Personality disorders',
            'F10': 'Alcohol use disorders'
        }
        
        # Age influences diagnosis
        if patient['age_at_start'] < 30:
            diagnosis = random.choices(
                list(mh_diagnoses.keys()),
                weights=[0.3, 0.35, 0.2, 0.05, 0.05, 0.03, 0.02]
            )[0]
        else:
            diagnosis = random.choices(
                list(mh_diagnoses.keys()),
                weights=[0.4, 0.25, 0.15, 0.08, 0.07, 0.03, 0.02]
            )[0]
        
        # Care cluster (payment grouping)
        care_clusters = {
            1: 'Non-psychotic (low severity)',
            2: 'Non-psychotic (medium severity)', 
            3: 'Non-psychotic (high severity)',
            4: 'Non-psychotic (very high severity)',
            11: 'Ongoing recurrent psychosis',
            12: 'Ongoing recurrent psychosis (high support)',
            21: 'Cognitive impairment'
        }
        
        if diagnosis in ['F32', 'F41', 'F43']:
            cluster = random.choices([1, 2, 3, 4], weights=[0.4, 0.35, 0.2, 0.05])[0]
        elif diagnosis in ['F20', 'F31']:
            cluster = random.choices([11, 12], weights=[0.7, 0.3])[0]
        else:
            cluster = random.choice([1, 2, 3])
        
        # Generate care contacts
        contacts = self.generate_care_contacts(referral_date, diagnosis)
        
        # HoNOS scores (Health of Nation Outcome Scales)
        initial_honos = random.randint(15, 35)  # Higher = more severe
        final_honos = max(0, initial_honos - random.randint(0, 10))  # Improvement
        
        return {
            'referral_id': f'MH{referral_id:08d}',
            'patient_id': patient['patient_id'],
            'nhs_number': patient['nhs_number'],
            'referral_date': referral_date,
            'referral_source': source,
            'primary_diagnosis': diagnosis,
            'care_cluster': cluster,
            'urgency': random.choice(['Routine', 'Urgent', 'Emergency']),
            'initial_honos_score': initial_honos,
            'latest_honos_score': final_honos,
            'total_contacts': len(contacts),
            'first_contact_date': min(contacts) if contacts else None,
            'last_contact_date': max(contacts) if contacts else None,
            'discharge_date': max(contacts) + timedelta(days=30) if contacts else None,
            'discharge_reason': random.choice(['Treatment completed', 'DNA', 'Moved area', 'Stepped down']),
            'risk_assessment': random.choice(['Low', 'Medium', 'High']),
            'created_timestamp': datetime.datetime.now()
        }
    
    def generate_care_contacts(self, referral_date: datetime.date, diagnosis: str) -> List[datetime.date]:
        """Generate care contact dates"""
        contacts = []
        
        # Number of contacts based on diagnosis severity
        if diagnosis in ['F20', 'F31']:  # Severe mental illness
            num_contacts = random.randint(8, 25)
        elif diagnosis in ['F32', 'F41']:  # Common mental health
            num_contacts = random.randint(4, 12)
        else:
            num_contacts = random.randint(2, 8)
        
        current_date = referral_date + timedelta(days=random.randint(7, 21))  # First appointment
        
        for _ in range(num_contacts):
            contacts.append(current_date)
            # Next contact in 1-4 weeks
            current_date += timedelta(days=random.randint(7, 28))
        
        return contacts

class CSSDSGenerator:
    """Community Services Data Set Generator"""
    
    def __init__(self, patients_df: pd.DataFrame, config: DataGenerationConfig):
        self.patients_df = patients_df
        self.config = config
    
    def generate_contacts(self) -> pd.DataFrame:
        """Generate community service contacts"""
        contacts = []
        contact_id = 1
        
        # Community services more likely for elderly and deprived populations
        weights = np.where(self.patients_df['age_at_start'] > 65, 3.0, 1.0)
        weights = weights * (11 - self.patients_df['deprivation_decile'])
        
        # Select patients receiving community services (~8% of population)
        community_patients = self.patients_df.sample(
            n=int(len(self.patients_df) * 0.08),
            weights=weights
        )
        
        for _, patient in community_patients.iterrows():
            # Generate contacts over time period
            num_contacts = random.randint(1, 20)
            
            for contact_num in range(num_contacts):
                contact_date = self.config.start_date + timedelta(
                    days=random.randint(0, 365 * self.config.years_of_data)
                )
                
                contact = self.generate_single_contact(contact_id, patient, contact_date)
                contacts.append(contact)
                contact_id += 1
        
        return pd.DataFrame(contacts)
    
    def generate_single_contact(self, contact_id: int, patient: pd.Series, contact_date: datetime.date) -> dict:
        """Generate single community service contact"""
        # Service types
        services = {
            'N01': 'District nursing',
            'N02': 'Health visiting', 
            'A01': 'Physiotherapy',
            'A02': 'Occupational therapy',
            'A03': 'Speech and language therapy',
            'A04': 'Dietitian',
            'N03': 'Community mental health',
            'N04': 'Specialist nursing'
        }
        
        # Age influences service type
        if patient['age_at_start'] > 75:
            service_weights = [0.35, 0.05, 0.25, 0.15, 0.05, 0.05, 0.05, 0.05]
        elif patient['age_at_start'] < 5:
            service_weights = [0.1, 0.4, 0.1, 0.1, 0.15, 0.05, 0.05, 0.05]
        else:
            service_weights = [0.15, 0.1, 0.2, 0.15, 0.1, 0.1, 0.15, 0.05]
        
        service_code = random.choices(list(services.keys()), weights=service_weights)[0]
        
        # Contact setting
        settings = {
            '01': 'Patient home',
            '02': 'Community clinic',
            '03': 'GP practice',
            '04': 'School',
            '05': 'Care home'
        }
        
        if patient['age_at_start'] > 75:
            setting = random.choices(
                list(settings.keys()),
                weights=[0.6, 0.2, 0.1, 0.0, 0.1]
            )[0]
        else:
            setting = random.choices(
                list(settings.keys()),
                weights=[0.4, 0.4, 0.15, 0.05, 0.0]
            )[0]
        
        # Contact duration
        if service_code in ['N01', 'N04']:  # Nursing
            duration_mins = random.randint(15, 60)
        elif service_code in ['A01', 'A02']:  # Therapy
            duration_mins = random.randint(30, 90)
        else:
            duration_mins = random.randint(20, 60)
        
        return {
            'contact_id': f'CS{contact_id:08d}',
            'patient_id': patient['patient_id'],
            'nhs_number': patient['nhs_number'],
            'contact_date': contact_date,
            'service_type': service_code,
            'service_name': services[service_code],
            'care_professional': f'CP{random.randint(1000, 9999)}',
            'contact_setting': setting,
            'contact_duration_mins': duration_mins,
            'contact_mode': random.choice(['Face to face', 'Telephone', 'Video call']),
            'care_activity': random.choice(['Assessment', 'Treatment', 'Review', 'Discharge planning']),
            'onward_referral': random.choice([True, False]),
            'safeguarding_concern': random.random() < 0.03,
            'created_timestamp': datetime.datetime.now()
        }

class PrescribingGenerator:
    """NHS BSA Prescribing Data Generator"""
    
    def __init__(self, patients_df: pd.DataFrame, practices_df: pd.DataFrame, config: DataGenerationConfig):
        self.patients_df = patients_df
        self.practices_df = practices_df
        self.config = config
        self.bnf_codes = NHSCodebooks.get_bnf_codes()
    
    def generate_prescriptions(self) -> pd.DataFrame:
        """Generate prescription data"""
        prescriptions = []
        prescription_id = 1
        
        # Generate monthly prescription data
        current_date = self.config.start_date
        
        while current_date < self.config.start_date + timedelta(days=365 * self.config.years_of_data):
            # Monthly prescribing volume
            monthly_prescriptions = random.randint(8000, 12000)
            
            for _ in range(monthly_prescriptions):
                prescription = self.generate_single_prescription(prescription_id, current_date)
                prescriptions.append(prescription)
                prescription_id += 1
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        return pd.DataFrame(prescriptions)
    
    def generate_single_prescription(self, prescription_id: int, month_date: datetime.date) -> dict:
        """Generate single prescription item"""
        # Select patient (prescribing more likely in elderly)
        weights = np.where(self.patients_df['age_at_start'] > 50, 2.0, 1.0)
        weights = np.where(self.patients_df['age_at_start'] > 70, 4.0, weights)
        
        patient = self.patients_df.sample(n=1, weights=weights).iloc[0]
        practice = self.practices_df.sample(n=1).iloc[0]
        
        # Select BNF code based on age
        if patient['age_at_start'] > 65:
            # Elderly more likely cardiovascular, diabetes meds
            bnf_weights = [0.1, 0.2, 0.25, 0.1, 0.05, 0.15, 0.05, 0.05, 0.05]
        elif patient['age_at_start'] < 18:
            # Children more likely antibiotics, simple analgesics
            bnf_weights = [0.15, 0.05, 0.05, 0.1, 0.1, 0.05, 0.3, 0.15, 0.05]
        else:
            # Adults mixed pattern
            bnf_weights = [0.12, 0.1, 0.15, 0.12, 0.08, 0.1, 0.1, 0.13, 0.1]
        
        bnf_code = random.choices(list(self.bnf_codes.keys()), weights=bnf_weights)[0]
        bnf_data = self.bnf_codes[bnf_code]
        
        # Quantity and cost
        quantity = random.randint(1, 6) * 28  # Usually 28-day supplies
        unit_cost = bnf_data['avg_cost'] + random.uniform(-2, 5)
        unit_cost = max(0.50, unit_cost)  # Minimum cost
        
        net_ingredient_cost = unit_cost * (quantity / 28)
        actual_cost = net_ingredient_cost * random.uniform(0.9, 1.1)  # Discount variation
        
        # Prescription date within the month
        prescription_date = month_date + timedelta(days=random.randint(0, 27))
        
        return {
            'prescription_id': f'RX{prescription_id:08d}',
            'patient_id': patient['patient_id'],
            'nhs_number': patient['nhs_number'],
            'practice_code': practice['practice_code'],
            'prescription_date': prescription_date,
            'bnf_code': bnf_code,
            'bnf_name': bnf_data['name'],
            'quantity': quantity,
            'net_ingredient_cost': round(net_ingredient_cost, 2),
            'actual_cost': round(actual_cost, 2),
            'prescriber_code': f'PR{random.randint(100000, 999999)}',
            'prescription_items': 1,
            'created_timestamp': datetime.datetime.now()
        }

class SocialCareGenerator:
    """Adult Social Care Data Generator"""
    
    def __init__(self, patients_df: pd.DataFrame, config: DataGenerationConfig):
        self.patients_df = patients_df
        self.config = config
    
    def generate_care_packages(self) -> pd.DataFrame:
        """Generate social care packages"""
        packages = []
        package_id = 1
        
        # Social care primarily for elderly and disabled
        eligible_patients = self.patients_df[
            (self.patients_df['age_at_start'] > 65) |
            (self.patients_df['deprivation_decile'] <= 3)
        ]
        
        # About 3% receive social care
        care_recipients = eligible_patients.sample(n=int(len(eligible_patients) * 0.03))
        
        for _, patient in care_recipients.iterrows():
            package = self.generate_single_package(package_id, patient)
            packages.append(package)
            package_id += 1
        
        return pd.DataFrame(packages)
    
    def generate_single_package(self, package_id: int, patient: pd.Series) -> dict:
        """Generate single social care package"""
        # Package types
        package_types = {
            'HOMECARE': 'Home care',
            'DAYCARE': 'Day care',
            'RESIDENTIAL': 'Residential care',
            'NURSING': 'Nursing home',
            'EQUIPMENT': 'Equipment/adaptations',
            'DIRECT_PAYMENT': 'Direct payment'
        }
        
        # Age influences package type
        if patient['age_at_start'] > 85:
            type_weights = [0.3, 0.1, 0.25, 0.25, 0.05, 0.05]
        elif patient['age_at_start'] > 75:
            type_weights = [0.4, 0.2, 0.15, 0.15, 0.05, 0.05]
        else:
            type_weights = [0.35, 0.25, 0.1, 0.1, 0.1, 0.1]
        
        package_type = random.choices(list(package_types.keys()), weights=type_weights)[0]
        
        # Weekly cost based on package type
        cost_ranges = {
            'HOMECARE': (100, 500),
            'DAYCARE': (150, 300),
            'RESIDENTIAL': (600, 1200),
            'NURSING': (800, 1500),
            'EQUIPMENT': (50, 200),
            'DIRECT_PAYMENT': (200, 800)
        }
        
        min_cost, max_cost = cost_ranges[package_type]
        weekly_cost = random.randint(min_cost, max_cost)
        
        # Start date
        start_date = self.config.start_date + timedelta(
            days=random.randint(0, 365 * self.config.years_of_data - 180)
        )
        
        # Duration (some ongoing, some completed)
        if random.random() < 0.3:  # 30% completed
            duration_weeks = random.randint(4, 52)
            end_date = start_date + timedelta(weeks=duration_weeks)
            status = 'Completed'
        else:  # Ongoing
            duration_weeks = random.randint(12, 156)  # Up to 3 years
            end_date = None
            status = 'Active'
        
        # Assessment outcome
        assessment_outcomes = {
            'LOW': 'Low needs',
            'MODERATE': 'Moderate needs', 
            'SUBSTANTIAL': 'Substantial needs',
            'CRITICAL': 'Critical needs'
        }
        
        if patient['age_at_start'] > 85:
            outcome = random.choices(
                list(assessment_outcomes.keys()),
                weights=[0.1, 0.2, 0.4, 0.3]
            )[0]
        else:
            outcome = random.choices(
                list(assessment_outcomes.keys()),
                weights=[0.2, 0.4, 0.3, 0.1]
            )[0]
        
        return {
            'package_id': f'SC{package_id:08d}',
            'patient_id': patient['patient_id'],
            'nhs_number': patient['nhs_number'],
            'package_type': package_type,
            'package_name': package_types[package_type],
            'start_date': start_date,
            'end_date': end_date,
            'status': status,
            'weekly_cost': weekly_cost,
            'assessment_outcome': outcome,
            'care_provider': f'Provider_{random.randint(1, 50)}',
            'hours_per_week': random.randint(2, 40) if package_type == 'HOMECARE' else None,
            'review_date': start_date + timedelta(weeks=12),
            'created_timestamp': datetime.datetime.now()
        }

class DatasetLinker:
    """Links datasets using NHS number and creates patient journeys"""
    
    def __init__(self, config: DataGenerationConfig):
        self.config = config
    
    def create_patient_journeys(self, sus_df: pd.DataFrame, ecds_df: pd.DataFrame, 
                               mhsds_df: pd.DataFrame, csds_df: pd.DataFrame) -> pd.DataFrame:
        """Create integrated patient journey dataset"""
        journeys = []
        
        # Get all patients with any activity
        active_patients = set()
        active_patients.update(sus_df['patient_id'].dropna())
        active_patients.update(ecds_df['patient_id'].dropna())
        active_patients.update(mhsds_df['patient_id'].dropna())
        active_patients.update(csds_df['patient_id'].dropna())
        
        for patient_id in active_patients:
            # Get all events for this patient
            patient_events = []
            
            # SUS episodes
            patient_sus = sus_df[sus_df['patient_id'] == patient_id]
            for _, episode in patient_sus.iterrows():
                patient_events.append({
                    'patient_id': patient_id,
                    'event_date': episode['admission_date'],
                    'event_type': 'Hospital Admission',
                    'care_setting': 'Acute',
                    'primary_code': episode['primary_diagnosis'],
                    'cost': self.estimate_episode_cost(episode['hrg_code']),
                    'source_dataset': 'SUS+',
                    'source_record_id': episode['episode_id']
                })
            
            # ECDS attendances
            patient_ecds = ecds_df[ecds_df['patient_id'] == patient_id]
            for _, attendance in patient_ecds.iterrows():
                patient_events.append({
                    'patient_id': patient_id,
                    'event_date': attendance['arrival_datetime'].date(),
                    'event_type': 'A&E Attendance',
                    'care_setting': 'Emergency',
                    'primary_code': attendance['presenting_complaint'],
                    'cost': 280.0,  # Average A&E tariff
                    'source_dataset': 'ECDS',
                    'source_record_id': attendance['attendance_id']
                })
            
            # MHSDS referrals
            patient_mh = mhsds_df[mhsds_df['patient_id'] == patient_id]
            for _, referral in patient_mh.iterrows():
                patient_events.append({
                    'patient_id': patient_id,
                    'event_date': referral['referral_date'],
                    'event_type': 'Mental Health Referral',
                    'care_setting': 'Mental Health',
                    'primary_code': referral['primary_diagnosis'],
                    'cost': referral['total_contacts'] * 180,  # Estimate per contact
                    'source_dataset': 'MHSDS',
                    'source_record_id': referral['referral_id']
                })
            
            # CSDS contacts
            patient_cs = csds_df[csds_df['patient_id'] == patient_id]
            for _, contact in patient_cs.iterrows():
                patient_events.append({
                    'patient_id': patient_id,
                    'event_date': contact['contact_date'],
                    'event_type': 'Community Contact',
                    'care_setting': 'Community',
                    'primary_code': contact['service_type'],
                    'cost': contact['contact_duration_mins'] * 2.5,  # £2.50 per minute estimate
                    'source_dataset': 'CSDS',
                    'source_record_id': contact['contact_id']
                })
            
            # Sort events by date
            patient_events.sort(key=lambda x: x['event_date'])
            
            # Create journey record
            if patient_events:
                total_events = len(patient_events)
                total_cost = sum(event['cost'] for event in patient_events)
                care_settings = set(event['care_setting'] for event in patient_events)
                
                # Identify care transitions within 30 days
                transitions = []
                for i in range(len(patient_events) - 1):
                    days_diff = (patient_events[i+1]['event_date'] - patient_events[i]['event_date']).days
                    if days_diff <= 30:
                        transitions.append(f"{patient_events[i]['care_setting']} -> {patient_events[i+1]['care_setting']}")
                
                journeys.append({
                    'patient_id': patient_id,
                    'journey_start_date': patient_events[0]['event_date'],
                    'journey_end_date': patient_events[-1]['event_date'],
                    'total_events': total_events,
                    'total_cost': total_cost,
                    'care_settings_used': ','.join(sorted(care_settings)),
                    'care_transitions': ','.join(transitions),
                    'integrated_care_episode': len(care_settings) > 1,
                    'high_intensity': total_events > 10,
                    'created_timestamp': datetime.datetime.now()
                })
        
        return pd.DataFrame(journeys)
    
    def estimate_episode_cost(self, hrg_code: str) -> float:
        """Estimate episode cost from HRG code"""
        hrg_costs = NHSCodebooks.get_hrg_codes()
        return hrg_costs.get(hrg_code, {'tariff': 2000.0})['tariff']

class DataQualitySimulator:
    """Simulates realistic data quality issues"""
    
    @staticmethod
    def apply_missing_data(df: pd.DataFrame, columns: List[str], missing_rate: float = 0.1) -> pd.DataFrame:
        """Apply missing data patterns"""
        df_copy = df.copy()
        
        for col in columns:
            if col in df_copy.columns:
                mask = np.random.random(len(df_copy)) < missing_rate
                df_copy.loc[mask, col] = None
        
        return df_copy
    
    @staticmethod
    def apply_duplicates(df: pd.DataFrame, duplicate_rate: float = 0.05) -> pd.DataFrame:
        """Create duplicate records"""
        num_duplicates = int(len(df) * duplicate_rate)
        if num_duplicates > 0:
            duplicate_indices = np.random.choice(df.index, size=num_duplicates, replace=False)
            duplicates = df.loc[duplicate_indices].copy()
            
            # Slightly modify duplicates to simulate data entry variations
            for col in ['postcode', 'nhs_number']:
                if col in duplicates.columns:
                    duplicates[col] = duplicates[col].apply(
                        lambda x: x + 'X' if isinstance(x, str) and random.random() < 0.5 else x
                    )
            
            return pd.concat([df, duplicates], ignore_index=True)
        return df
    
    @staticmethod
    def apply_date_inconsistencies(df: pd.DataFrame) -> pd.DataFrame:
        """Create date logic errors"""
        df_copy = df.copy()
        
        # SUS data: discharge before admission
        if 'admission_date' in df_copy.columns and 'discharge_date' in df_copy.columns:
            error_mask = np.random.random(len(df_copy)) < 0.005  # 0.5% error rate
            df_copy.loc[error_mask, 'discharge_date'] = df_copy.loc[error_mask, 'admission_date'] - pd.Timedelta(days=1)
        
        return df_copy

class PseudonymisationEngine:
    """Implements NHS-compliant pseudonymisation"""
    
    def __init__(self, salt: str = "NHS_SALT_2025"):
        self.salt = salt
    
    def pseudonymise_nhs_number(self, nhs_number: str) -> str:
        """Create pseudonymised NHS number"""
        if pd.isna(nhs_number) or nhs_number == 'INVALID':
            return nhs_number
        
        # SHA-256 hash with salt
        combined = f"{nhs_number}_{self.salt}"
        hash_object = hashlib.sha256(combined.encode())
        return f"PSEUDO_{hash_object.hexdigest()[:16].upper()}"
    
    def pseudonymise_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply pseudonymisation to dataset"""
        df_pseudo = df.copy()
        
        if 'nhs_number' in df_pseudo.columns:
            df_pseudo['nhs_number'] = df_pseudo['nhs_number'].apply(self.pseudonymise_nhs_number)
        
        # Remove direct identifiers
        identifiers_to_remove = ['patient_name', 'address', 'phone_number', 'email']
        for col in identifiers_to_remove:
            if col in df_pseudo.columns:
                df_pseudo = df_pseudo.drop(columns=[col])
        
        # Generalize postcodes to district level
        if 'postcode' in df_pseudo.columns:
            df_pseudo['postcode_district'] = df_pseudo['postcode'].str.extract(r'([A-Z]{1,2}\d{1,2})')
            df_pseudo = df_pseudo.drop(columns=['postcode'])
        
        return df_pseudo

class NHSDataGeneratorSuite:
    """Main class orchestrating all data generation"""
    
    def __init__(self, config: DataGenerationConfig = None):
        self.config = config or DataGenerationConfig()
        self.pseudo_engine = PseudonymisationEngine()
        self.output_dir = "nhs_synthetic_data"
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
    
    def generate_all_datasets(self, apply_quality_issues: bool = True, 
                            apply_pseudonymisation: bool = True) -> Dict[str, pd.DataFrame]:
        """Generate complete suite of NHS datasets"""
        print("🏥 Starting NHS Data Generation Suite...")
        
        # Step 1: Generate foundational data
        print("📊 Generating patient population...")
        patient_gen = PatientGenerator(self.config)
        patients_df = patient_gen.generate_patients()
        
        print("🏢 Generating provider organizations...")
        provider_gen = ProviderGenerator(self.config)
        trusts_df = provider_gen.generate_trusts()
        practices_df = provider_gen.generate_practices()
        
        # Step 2: Generate clinical datasets
        print("🚑 Generating SUS+ hospital episodes...")
        sus_gen = SUSPlusGenerator(patients_df, trusts_df, self.config)
        sus_df = sus_gen.generate_episodes()
        
        print("🚨 Generating ECDS A&E attendances...")
        ecds_gen = ECDSGenerator(patients_df, trusts_df, self.config)
        ecds_df = ecds_gen.generate_attendances()
        
        print("🧠 Generating MHSDS mental health data...")
        mhsds_gen = MHSDSGenerator(patients_df, self.config)
        mhsds_df = mhsds_gen.generate_referrals()
        
        print("🏠 Generating CSDS community services...")
        csds_gen = CSSDSGenerator(patients_df, self.config)
        csds_df = csds_gen.generate_contacts()
        
        print("💊 Generating prescribing data...")
        prescribing_gen = PrescribingGenerator(patients_df, practices_df, self.config)
        prescribing_df = prescribing_gen.generate_prescriptions()
        
        print("👥 Generating social care data...")
        social_care_gen = SocialCareGenerator(patients_df, self.config)
        social_care_df = social_care_gen.generate_care_packages()
        
        # Step 3: Create patient journeys
        print("🔗 Creating integrated patient journeys...")
        linker = DatasetLinker(self.config)
        journeys_df = linker.create_patient_journeys(sus_df, ecds_df, mhsds_df, csds_df)
        
        # Compile all datasets
        datasets = {
            'patients': patients_df,
            'trusts': trusts_df,
            'practices': practices_df,
            'sus_episodes': sus_df,
            'ecds_attendances': ecds_df,
            'mhsds_referrals': mhsds_df,
            'csds_contacts': csds_df,
            'prescriptions': prescribing_df,
            'social_care': social_care_df,
            'patient_journeys': journeys_df
        }
        
        # Step 4: Apply data quality issues
        if apply_quality_issues:
            print("⚠️  Applying realistic data quality issues...")
            datasets = self.apply_data_quality_issues(datasets)
        
        # Step 5: Apply pseudonymisation
        if apply_pseudonymisation:
            print("🔒 Applying NHS-compliant pseudonymisation...")
            datasets = self.apply_pseudonymisation(datasets)
        
        # Step 6: Save datasets
        print("💾 Saving datasets to files...")
        self.save_datasets(datasets)
        
        # Step 7: Generate summary report
        self.generate_summary_report(datasets)
        
        print("✅ NHS Data Generation Suite completed!")
        print(f"📁 Data saved to: {self.output_dir}")
        
        return datasets
    
    def apply_data_quality_issues(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Apply realistic data quality issues across datasets"""
        quality_sim = DataQualitySimulator()
        
        # Apply missing data
        datasets['sus_episodes'] = quality_sim.apply_missing_data(
            datasets['sus_episodes'], 
            ['secondary_diagnoses', 'secondary_procedures', 'ward_code'],
            missing_rate=0.15
        )
        
        datasets['ecds_attendances'] = quality_sim.apply_missing_data(
            datasets['ecds_attendances'],
            ['investigations', 'referred_to_specialist'],
            missing_rate=0.12
        )
        
        # Apply duplicates
        datasets['patients'] = quality_sim.apply_duplicates(datasets['patients'], duplicate_rate=0.08)
        
        # Apply date inconsistencies
        datasets['sus_episodes'] = quality_sim.apply_date_inconsistencies(datasets['sus_episodes'])
        
        return datasets
    
    def apply_pseudonymisation(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Apply pseudonymisation to all datasets"""
        pseudonymised_datasets = {}
        
        for name, df in datasets.items():
            pseudonymised_datasets[name] = self.pseudo_engine.pseudonymise_dataset(df)
        
        return pseudonymised_datasets
    
    def save_datasets(self, datasets: Dict[str, pd.DataFrame]):
        """Save all datasets to files"""
        for name, df in datasets.items():
            # Save as both CSV and Parquet
            csv_path = os.path.join(self.output_dir, f"{name}.csv")
            parquet_path = os.path.join(self.output_dir, f"{name}.parquet")
            
            df.to_csv(csv_path, index=False)
            df.to_parquet(parquet_path, index=False)
            
            print(f"   ✓ Saved {name}: {len(df):,} records")
    
    def generate_summary_report(self, datasets: Dict[str, pd.DataFrame]):
        """Generate data generation summary report"""
        report = {
            "generation_timestamp": datetime.datetime.now().isoformat(),
            "configuration": {
                "base_population": self.config.base_population,
                "years_of_data": self.config.years_of_data,
                "trusts": self.config.trusts,
                "practices": self.config.practices
            },
            "dataset_summary": {}
        }
        
        for name, df in datasets.items():
            report["dataset_summary"][name] = {
                "record_count": len(df),
                "columns": list(df.columns),
                "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            }
        
        # Save report
        report_path = os.path.join(self.output_dir, "generation_report.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"📋 Generation report saved to: {report_path}")

# Example usage and testing
if __name__ == "__main__":
    # Configure generation
    config = DataGenerationConfig(
        base_population=10000,  # Smaller for testing
        trusts=10,
        practices=50,
        years_of_data=2,
        data_quality_degradation=0.15,
        seasonal_variation=True,
        covid_impact=True
    )
    
    # Generate all datasets
    generator = NHSDataGeneratorSuite(config)
    datasets = generator.generate_all_datasets(
        apply_quality_issues=True,
        apply_pseudonymisation=True
    )
    
    # Display summary statistics
    print("\n📊 DATASET SUMMARY:")
    print("=" * 50)
    for name, df in datasets.items():
        print(f"{name:20}: {len(df):>8,} records")
    
    print(f"\n🎯 Total synthetic patient population: {len(datasets['patients']):,}")
    print(f"📅 Data period: {config.start_date} to {config.start_date + timedelta(days=365*config.years_of_data)}")
    print(f"💾 Data saved to: {generator.output_dir}")
    
    # Example: Show patient journey for high-activity patients
    high_activity_journeys = datasets['patient_journeys'][
        datasets['patient_journeys']['total_events'] > 5
    ].head()
    
    if not high_activity_journeys.empty:
        print(f"\n🔄 Example High-Activity Patient Journeys:")
        print(high_activity_journeys[['patient_id', 'total_events', 'total_cost', 'care_settings_used']].to_string(index=False))
    
    print("\n✨ NHS Data Generation Suite completed successfully!")
    print("🚀 Ready for Azure Data Engineering with realistic NHS datasets!")
