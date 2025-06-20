import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime, timedelta
import random
import pymysql
import logging
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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
        self.color_palette = px.colors.qualitative.Plotly
        plt.style.use('ggplot')

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
            SUM(CASE WHEN f.status_absen = 'Absen' THEN 1 ELSE 0 END) AS absen,
            SUM(CASE WHEN f.status_absen = 'Izin' THEN 1 ELSE 0 END) AS izin,
            ROUND(AVG(f.terlambat_menit), 1) AS rata_terlambat,
            ROUND(AVG(f.lembur_menit), 1) AS rata_lembur,
            ROUND(SUM(CASE WHEN f.status_absen IN ('Hadir', 'Terlambat') THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS persentase_hadir
        FROM fakta_absensi f
        JOIN dim_waktu w ON f.id_waktu = w.id_waktu
        JOIN dim_departemen d ON f.id_departemen = d.id_departemen
        GROUP BY w.tahun, w.bulan, d.nama_departemen
        ORDER BY w.tahun, w.bulan
        """
        return pd.read_sql(query, self.engine)
    
    def get_daily_attendance(self):
        query = """
        SELECT 
            w.tanggal,
            w.hari,
            d.nama_departemen AS departemen,
            COUNT(*) AS total_absen,
            SUM(CASE WHEN f.status_absen = 'Hadir' THEN 1 ELSE 0 END) AS hadir,
            SUM(CASE WHEN f.status_absen = 'Terlambat' THEN 1 ELSE 0 END) AS terlambat,
            ROUND(AVG(f.terlambat_menit), 1) AS rata_terlambat,
            ROUND(SUM(CASE WHEN f.status_absen IN ('Hadir', 'Terlambat') THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS persentase_hadir
        FROM fakta_absensi f
        JOIN dim_waktu w ON f.id_waktu = w.id_waktu
        JOIN dim_departemen d ON f.id_departemen = d.id_departemen
        GROUP BY w.tanggal, w.hari, d.nama_departemen
        ORDER BY w.tanggal
        """
        return pd.read_sql(query, self.engine)
    
    def get_employee_stats(self):
        query = """
        SELECT 
            k.nip,
            k.nama,
            k.departemen,
            k.jabatan,
            COUNT(*) AS total_hari_kerja,
            SUM(CASE WHEN f.status_absen = 'Hadir' THEN 1 ELSE 0 END) AS hadir,
            SUM(CASE WHEN f.status_absen = 'Terlambat' THEN 1 ELSE 0 END) AS terlambat,
            SUM(CASE WHEN f.status_absen = 'Absen' THEN 1 ELSE 0 END) AS absen,
            ROUND(SUM(f.terlambat_menit), 1) AS total_terlambat_menit,
            ROUND(AVG(f.terlambat_menit), 1) AS rata_terlambat,
            ROUND(SUM(f.lembur_menit)/60, 1) AS total_lembur_jam
        FROM fakta_absensi f
        JOIN dim_karyawan k ON f.id_karyawan = k.id_karyawan
        GROUP BY k.nip, k.nama, k.departemen, k.jabatan
        ORDER BY total_terlambat_menit DESC
        LIMIT 20
        """
        return pd.read_sql(query, self.engine)
    
    def get_shift_stats(self):
        query = """
        SELECT 
            s.kode_shift,
            s.deskripsi,
            COUNT(*) AS total_absen,
            SUM(CASE WHEN f.status_absen = 'Hadir' THEN 1 ELSE 0 END) AS hadir,
            SUM(CASE WHEN f.status_absen = 'Terlambat' THEN 1 ELSE 0 END) AS terlambat,
            ROUND(AVG(f.terlambat_menit), 1) AS rata_terlambat,
            ROUND(AVG(f.lembur_menit), 1) AS rata_lembur,
            ROUND(SUM(CASE WHEN f.status_absen IN ('Hadir', 'Terlambat') THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS persentase_hadir
        FROM fakta_absensi f
        JOIN dim_shift s ON f.id_shift = s.id_shift
        GROUP BY s.kode_shift, s.deskripsi
        ORDER BY total_absen DESC
        """
        return pd.read_sql(query, self.engine)

    def generate_dashboard(self):
        logging.info("Generating comprehensive dashboard...")
        
        # Get all data
        monthly_data = self.get_monthly_summary()
        daily_data = self.get_daily_attendance()
        employee_data = self.get_employee_stats()
        shift_data = self.get_shift_stats()
        
        # Validasi data sebelum visualisasi
        self._validate_data(employee_data)
        
        # Create figures for each visualization
        self._create_monthly_trend(monthly_data)
        self._create_attendance_distribution(monthly_data)
        self._create_shift_analysis(shift_data)
        self._create_employee_lateness(employee_data)
        self._create_daily_heatmap(daily_data)
        
        logging.info("Dashboard components saved as separate HTML files")

    def _validate_data(self, employee_data):
        """Validasi konsistensi data karyawan"""
        # Cek duplikasi NIP
        if employee_data['nip'].duplicated().any():
            logging.warning("Ada duplikasi NIP dalam data karyawan")
        
        # Cek konsistensi departemen
        dept_counts = employee_data.groupby(['nama', 'departemen']).size().reset_index(name='counts')
        duplicates = dept_counts[dept_counts.duplicated(['nama'], keep=False)]
        
        if not duplicates.empty:
            logging.warning(f"Ada ketidakonsistenan departemen untuk karyawan: {duplicates['nama'].unique()}")
            # Ambil data asli dari database untuk verifikasi
            query = """
            SELECT DISTINCT k.nip, k.nama, k.departemen 
            FROM dim_karyawan k
            JOIN fakta_absensi f ON k.id_karyawan = f.id_karyawan
            WHERE k.nama IN %s
            """
            with self.engine.connect() as conn:
                actual_data = pd.read_sql(query, conn, params=(tuple(duplicates['nama'].unique(),)))
            
            logging.info(f"Data aktual dari database:\n{actual_data}")
            
            # Update data yang akan divisualisasikan
            for _, row in duplicates.iterrows():
                correct_dept = actual_data[actual_data['nama'] == row['nama']]['departemen'].iloc[0]
                employee_data.loc[employee_data['nama'] == row['nama'], 'departemen'] = correct_dept

    def _create_monthly_trend(self, data):
        fig = px.line(
            data,
            x='bulan_tahun',
            y='persentase_hadir',
            color='departemen',
            markers=True,
            title='Tren Kehadiran Bulanan per Departemen',
            labels={'persentase_hadir': 'Persentase Kehadiran (%)', 'bulan_tahun': 'Bulan-Tahun'},
            color_discrete_sequence=self.color_palette
        )
        fig.update_layout(
            hovermode='x unified',
            yaxis_range=[0, 100]
        )
        fig.write_html("monthly_attendance_trend.html")

    def _create_attendance_distribution(self, data):
        status_counts = data.groupby('departemen')[['hadir', 'terlambat', 'absen', 'izin']].sum().reset_index()
        fig = px.bar(
            status_counts.melt(id_vars='departemen', var_name='status', value_name='count'),
            x='departemen',
            y='count',
            color='status',
            barmode='group',
            title='Distribusi Status Absensi per Departemen',
            labels={'count': 'Jumlah Absensi', 'departemen': 'Departemen'},
            color_discrete_map={
                'hadir': self.color_palette[0],
                'terlambat': self.color_palette[1],
                'absen': self.color_palette[2],
                'izin': self.color_palette[3]
            }
        )
        fig.update_layout(
            xaxis_title="Departemen",
            yaxis_title="Jumlah Absensi (Bulan May - June)",
            legend_title="Status Absensi"
        )
        fig.write_html("attendance_distribution.html")

    def _create_shift_analysis(self, data):
        fig = px.bar(
            data,
            x='kode_shift',
            y='rata_terlambat',
            text='rata_terlambat',
            title='Rata-rata Keterlambatan per Shift Kerja',
            labels={'rata_terlambat': 'Rata-rata Keterlambatan (menit)', 'kode_shift': 'Shift'},
            color='kode_shift',
            color_discrete_sequence=self.color_palette[4:7]
        )
        fig.update_traces(
            texttemplate='%{text:.1f} menit', 
            textposition='outside',
            marker_line_color='rgb(8,48,107)',
            marker_line_width=1.5
        )
        fig.update_layout(
            showlegend=False,
            yaxis_title="Rata-rata Keterlambatan (menit)"
        )
        fig.write_html("shift_analysis.html")

    def _create_employee_lateness(self, data):
        # Validasi dan persiapkan data
        if data.empty:
            logging.warning("Data karyawan kosong, tidak dapat membuat visualisasi")
            return
            
        # Pastikan tidak ada duplikat nama karyawan
        if data['nama'].duplicated().any():
            logging.warning("Ada nama karyawan yang duplikat, menambahkan NIP untuk membedakan")
            data['nama_tampil'] = data['nama'] + ' (' + data['nip'] + ')'
        else:
            data['nama_tampil'] = data['nama']
        
        # Urutkan berdasarkan total keterlambatan
        data = data.sort_values('total_terlambat_menit', ascending=False)
        
        # Buat visualisasi
        fig = px.bar(
            data,
            x='nama_tampil',
            y='total_terlambat_menit',
            color='departemen',
            title='Top 20 Karyawan dengan Total Keterlambatan Tertinggi',
            labels={
                'total_terlambat_menit': 'Total Menit Terlambat',
                'nama_tampil': 'Nama Karyawan',
                'departemen': 'Departemen'
            },
            hover_data=['nip', 'jabatan', 'total_hari_kerja', 'rata_terlambat', 'total_lembur_jam'],
            color_discrete_sequence=self.color_palette
        )
        
        # Format tooltip
        fig.update_traces(
            hovertemplate=(
                "<b>%{x}</b><br>"
                "NIP: %{customdata[0]}<br>"
                "Departemen: %{marker.color}<br>"
                "Jabatan: %{customdata[1]}<br>"
                "Total Hari Kerja: %{customdata[2]}<br>"
                "Rata-rata Keterlambatan: %{customdata[3]:.1f} menit/hari<br>"
                "Total Lembur: %{customdata[4]:.1f} jam<br>"
                "<extra>Total Menit Terlambat: %{y}</extra>"
            )
        )
        
        fig.update_layout(
            xaxis_title="Nama Karyawan",
            yaxis_title="Total Menit Terlambat",
            legend_title="Departemen",
            xaxis={'categoryorder':'total descending'},
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
                font_family="Arial"
            )
        )
        
        # Rotasi label nama karyawan jika terlalu panjang
        fig.update_xaxes(tickangle=45)
        
        fig.write_html("employee_lateness.html")

    def _create_daily_heatmap(self, data):
        daily_pivot = data.pivot_table(
            index='hari', 
            columns='departemen', 
            values='persentase_hadir', 
            aggfunc='mean'
        ).reset_index()
        
        # Urutkan hari sesuai urutan mingguan
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        daily_pivot['hari'] = pd.Categorical(daily_pivot['hari'], categories=days_order, ordered=True)
        daily_pivot = daily_pivot.sort_values('hari').set_index('hari')
        
        fig = px.imshow(
            daily_pivot,
            labels=dict(x="Departemen", y="Hari", color="Persentase Kehadiran"),
            title='Tingkat Kehadiran Harian per Departemen',
            color_continuous_scale='Blues',
            aspect='auto'
        )
        
        # Format tooltip
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>Departemen: %{x}<br>Persentase Kehadiran: %{z:.2f}%"
        )
        
        fig.update_layout(
            xaxis_nticks=len(daily_pivot.columns),
            yaxis_nticks=7
        )
        fig.write_html("daily_heatmap.html")

if __name__ == "__main__":
    try:
        etl = AttendanceETL()
        if etl.run_etl():
            dashboard = AttendanceDashboard()
            dashboard.generate_dashboard()
            print("\n✅ ETL & Dashboard generation berhasil!")
            print("📊 Output files:")
            print("- monthly_attendance_trend.html")
            print("- attendance_distribution.html")
            print("- shift_analysis.html")
            print("- employee_lateness.html")
            print("- daily_heatmap.html")
        else:
            print("\n❌ ETL Gagal")
    except Exception as e:
        print(f"\n🔥 CRITICAL ERROR: {e}")
