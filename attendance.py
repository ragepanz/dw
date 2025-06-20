import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime, timedelta
import random
import pymysql
import logging
from typing import Dict

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('attendance_etl.log'), logging.StreamHandler()]
)

# DB Config
DB_CONFIG = {
    'host': 'localhost',
    'port': 13306,
    'database': 'absensi_akademik',
    'user': 'absensi_user',
    'password': 'absensi123',
    'auth_plugin': 'mysql_native_password'
}

class AttendanceETL:
    def __init__(self):
        self.engine = self._create_db_engine()
        self.dim_tables = ['dim_karyawan', 'dim_waktu', 'dim_shift', 'dim_departemen']
        self.fact_table = 'fakta_absensi'

    def _create_db_engine(self):
        try:
            engine = create_engine(
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
                connect_args={'ssl_disabled': True},
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True
            )
            logging.info("Database engine created successfully")
            return engine
        except Exception as e:
            logging.error(f"Failed to create database engine: {e}")
            raise

    def generate_sample_data(self) -> Dict:
        logging.info("Generating sample data...")
        departments = ['HR', 'Finance', 'IT', 'Operations', 'Marketing']
        locations = ['Head Office', 'Branch 1', 'Branch 2']
        employees = []
        for i in range(1, 21):
            employees.append({
                'nip': f'EMP{i:04d}',
                'nama': f'Employee {i}',
                'departemen': random.choice(departments),
                'jabatan': random.choice(['Staff', 'Supervisor', 'Manager']),
                'status_kerja': random.choice(['Permanent', 'Contract']),
                'join_date': (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
            })
        shifts = [
            {'kode_shift': 'PAGI', 'jam_masuk': '08:00', 'jam_keluar': '16:00', 'deskripsi': 'Shift Pagi'},
            {'kode_shift': 'SIANG', 'jam_masuk': '13:00', 'jam_keluar': '21:00', 'deskripsi': 'Shift Siang'},
            {'kode_shift': 'MALAM', 'jam_masuk': '21:00', 'jam_keluar': '05:00', 'deskripsi': 'Shift Malam'}
        ]
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        date_range = pd.date_range(start_date, end_date)
        attendance_data = []
        for date in date_range:
            if date.weekday() < 5:
                for emp in employees:
                    shift = random.choice(shifts)
                    status = random.choices(['Hadir', 'Terlambat', 'Absen', 'Izin'], weights=[0.7, 0.15, 0.1, 0.05])[0]
                    if status in ['Hadir', 'Terlambat']:
                        masuk = datetime.strptime(shift['jam_masuk'], '%H:%M')
                        if status == 'Terlambat':
                            masuk += timedelta(minutes=random.randint(5, 120))
                        keluar = datetime.strptime(shift['jam_keluar'], '%H:%M') + timedelta(
                            minutes=random.randint(-30, 120))
                        start_time = datetime.combine(date.date(), datetime.strptime(shift['jam_masuk'], '%H:%M').time())
                        arrival_time = datetime.combine(date.date(), masuk.time())
                        terlambat = max(0, (arrival_time - start_time).total_seconds() // 60)
                        end_time = datetime.combine(date.date(), datetime.strptime(shift['jam_keluar'], '%H:%M').time())
                        leave_time = datetime.combine(date.date(), keluar.time())
                        lembur = max(0, (leave_time - end_time).total_seconds() // 60)
                        attendance_data.append({
                            'nip': emp['nip'],
                            'tanggal': date.date(),
                            'kode_shift': shift['kode_shift'],
                            'status_absen': status,
                            'waktu_masuk': masuk.strftime('%H:%M'),
                            'waktu_keluar': keluar.strftime('%H:%M'),
                            'terlambat_menit': int(terlambat),
                            'lembur_menit': int(lembur)
                        })
                    else:
                        attendance_data.append({
                            'nip': emp['nip'],
                            'tanggal': date.date(),
                            'kode_shift': shift['kode_shift'],
                            'status_absen': status,
                            'waktu_masuk': None,
                            'waktu_keluar': None,
                            'terlambat_menit': None,
                            'lembur_menit': None
                        })
        return {
            'employees': employees,
            'shifts': shifts,
            'departments': [{'nama_departemen': d, 'lokasi': random.choice(locations)} for d in departments],
            'attendance': attendance_data
        }

    def transform(self, raw_data: Dict) -> Dict:
        logging.info("Transforming data...")
        df_emp = pd.DataFrame(raw_data['employees'])
        df_shift = pd.DataFrame(raw_data['shifts'])
        df_dept = pd.DataFrame(raw_data['departments'])
        df_att = pd.DataFrame(raw_data['attendance'])
        df_att['tanggal'] = pd.to_datetime(df_att['tanggal'], errors='coerce')
        df_att = df_att.dropna(subset=['tanggal'])
        for col in ['nip', 'nama', 'departemen', 'jabatan', 'status_kerja']:
            df_emp[col] = df_emp[col].astype(str).str.upper().str.strip()
        for col in ['kode_shift', 'deskripsi']:
            df_shift[col] = df_shift[col].astype(str).str.upper().str.strip()
        for col in ['nama_departemen', 'lokasi']:
            df_dept[col] = df_dept[col].astype(str).str.upper().str.strip()
        df_emp['id_karyawan'] = range(1, len(df_emp) + 1)
        df_shift['id_shift'] = range(1, len(df_shift) + 1)
        df_dept['id_departemen'] = range(1, len(df_dept) + 1)
        unique_dates = df_att['tanggal'].unique()
        df_date = pd.DataFrame({
            'tanggal': unique_dates,
            'hari': [d.strftime('%A') for d in unique_dates],
            'bulan': [d.strftime('%B') for d in unique_dates],
            'tahun': [d.year for d in unique_dates],
            'hari_kerja': [d.weekday() < 5 for d in unique_dates],
            'kuartal': [d.quarter for d in unique_dates]
        })
        df_date['id_waktu'] = range(1, len(df_date) + 1)
        df_fact = (
            df_att.merge(df_emp[['nip', 'id_karyawan', 'departemen']], on='nip')
                 .merge(df_shift[['kode_shift', 'id_shift']], on='kode_shift')
                 .merge(df_date, on='tanggal')
                 .merge(df_dept, left_on='departemen', right_on='nama_departemen')
        )
        df_fact['id_absensi'] = range(1, len(df_fact) + 1)
        return {
            'dim_karyawan': df_emp[['id_karyawan', 'nip', 'nama', 'departemen', 'jabatan', 'status_kerja', 'join_date']],
            'dim_waktu': df_date[['id_waktu', 'tanggal', 'hari', 'bulan', 'tahun', 'hari_kerja', 'kuartal']],
            'dim_shift': df_shift[['id_shift', 'kode_shift', 'jam_masuk', 'jam_keluar', 'deskripsi']],
            'dim_departemen': df_dept[['id_departemen', 'nama_departemen', 'lokasi']],
            'fakta_absensi': df_fact[[
                'id_absensi', 'id_karyawan', 'id_waktu', 'id_shift', 'id_departemen',
                'status_absen', 'waktu_masuk', 'waktu_keluar', 'terlambat_menit', 'lembur_menit'
            ]]
        }

    def load(self, transformed_data: Dict):
        logging.info("Loading data into data warehouse...")
        with self.engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table in self.dim_tables:
                logging.info(f"Loading {table}...")
                transformed_data[table].to_sql(table, conn, if_exists='replace', index=False)
            logging.info(f"Loading {self.fact_table}...")
            transformed_data[self.fact_table].to_sql(self.fact_table, conn, if_exists='replace', index=False)
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        logging.info("Data loading completed successfully")

    def run_etl(self):
        logging.info("=== Starting ETL Pipeline ===")
        data = self.generate_sample_data()
        transformed = self.transform(data)
        self.load(transformed)
        logging.info("=== ETL Completed ===")
        return True

class AttendanceDashboard:
    def __init__(self):
        self.engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
            connect_args={'ssl_disabled': True}
        )

    def get_monthly_summary(self):
        query = """
        SELECT 
            CONCAT(w.bulan, ' ', w.tahun) AS bulan_tahun,
            w.bulan,
            w.tahun,
            d.nama_departemen AS departemen,
            COUNT(*) AS total_absen,
            SUM(CASE WHEN f.status_absen = 'Hadir' THEN 1 ELSE 0 END) AS hadir,
            SUM(CASE WHEN f.status_absen = 'Terlambat' THEN 1 ELSE 0 END) AS terlambat,
            ROUND(AVG(f.terlambat_menit), 1) AS rata_terlambat,
            ROUND(AVG(f.lembur_menit), 1) AS rata_lembur,
            ROUND(SUM(CASE WHEN f.status_absen IN ('Hadir', 'Terlambat') THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS persentase_hadir
        FROM fakta_absensi f
        JOIN dim_waktu w ON f.id_waktu = w.id_waktu
        JOIN dim_departemen d ON f.id_departemen = d.id_departemen
        GROUP BY w.tahun, w.bulan, bulan_tahun, d.nama_departemen
        ORDER BY w.tahun, w.bulan
        """
        return pd.read_sql(query, self.engine)

    def generate_dashboard(self):
        logging.info("Generating dashboard...")
        df = self.get_monthly_summary()
        fig = px.line(
            df,
            x='bulan_tahun',
            y='persentase_hadir',
            color='departemen',
            markers=True,
            title='Tren Kehadiran Bulanan per Departemen'
        )
        fig.write_html("dashboard_bulanan.html")
        logging.info("Dashboard saved to dashboard_bulanan.html")

if __name__ == "__main__":
    try:
        etl = AttendanceETL()
        if etl.run_etl():
            dashboard = AttendanceDashboard()
            dashboard.generate_dashboard()
            print("\n✅ ETL & Dashboard generation berhasil!")
            print("📊 Output: dashboard_bulanan.html")
        else:
            print("\n❌ ETL Gagal")
    except Exception as e:
        print(f"\n🔥 CRITICAL ERROR: {e}")
