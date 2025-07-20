"""
=================================================

Program ini dibuat untuk automatisasi pengerjaan ETL pipeline, berikut adalah skemanya:
    - ETL pipeline: Postgres → Transform (extended) → Elasticsearch
Dataset: Global Music Streaming Trends & Listener Insights (2018–2024)

=================================================
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from elasticsearch import Elasticsearch, helpers

default_args = {
    'owner': 'yoseph_radityo',
    'start_date': datetime(2024, 11, 1),
}

with DAG(
    dag_id='P2M3_Yoseph_Radityo_DAG',
    description='ETL pipeline: Postgres → Transform → Elasticsearch',
    schedule_interval='10-30/10 9 * * 6',  # Setiap 10 menit antara 09:10-09:30 setiap Sabtu
    start_date=datetime(2024, 11, 1),  # Dimulai 1 November 2024
    catchup=False,
    default_args=default_args
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def extract():
        '''
        Fungsi ini bertujuan untuk mengambil data dari PostgreSQL dan menyimpannya
        dalam bentuk file CSV sementara.
        
        Proses:
        1. Membuat koneksi ke database menggunakan SQLAlchemy
        2. Membaca seluruh data dari tabel 'table_m3'
        3. Menyimpan data mentah ke file CSV temporer
        
        Returns:
            str: Path/lokasi file CSV mentah yang disimpan
        '''
        pg_user, pg_pass, pg_host, pg_db = 'airflow', 'airflow', 'postgres', 'airflow'
        url = f'postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5432/{pg_db}'
        engine = create_engine(url)
        df = pd.read_sql('SELECT * FROM table_m3;', con=engine)

        raw_path = '/tmp/P2M3_Yoseph_Radityo_data_raw.csv'
        df.to_csv(raw_path, index=False)
        print(f"[extract] saved raw CSV: {raw_path}")
        return raw_path

    @task()
    def transform(raw_path: str) -> str:
        '''
        Fungsi ini melakukan transformasi dan pembersihan data dari file mentah.
        
        Parameters:
            raw_path (str): Path/lokasi file CSV mentah dari proses extract
        
        Proses transformasi:
        1. Konversi teks ke lowercase dan stripping whitespace
        2. Normalisasi nama kolom
        3. Penghapusan kolom tidak diperlukan
        4. Rename kolom persentase
        5. Standarisasi format listening_time
        6. Kategorisasi usia ke dalam kelompok umur
        7. Pemetaan region berdasarkan negara
        8. Penanganan duplikat dan nilai null
        9. Konversi tipe data untuk konsistensi
        
        
        '''
        df = pd.read_csv(raw_path)
        
        # 1. Melakukan lowercase semua teks yang ada dalam dataset
        for c in df.select_dtypes(['object']).columns:
            df[c] = df[c].str.lower().str.strip()
        
        # 2. Melakukan normalisasi nama kolom
        df.columns = [c.lower().replace(' ', '_').replace('(%)', '') 
                      for c in df.columns]
        
        # 3. Dilakukan drop pada kolom yang tidak perlu
        df = df.drop(columns=['user_id'], errors='ignore')
        
        # 4. Melakukan rename kolom persen
        df.rename(columns={
            'discover_weekly_engagement': 'discover_engagement',
            'repeat_song_rate': 'repeat_rate'
        }, inplace=True)
        
        # 5. Pertahankan listening_time sebagai string
        df['listening_time'] = df['listening_time'].str.lower()
        
        # 6. Grup rentang usia 
        bins = [0, 18, 25, 35, 50, 100]
        labels = ['<18', '18-25', '26-35', '36-50', '50+']
        df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels)
        
        # 7. Map region (pertahankan sebagai string)
        region_map = {
            'japan': 'asia', 'south korea': 'asia', 'india': 'asia',
            'australia': 'oceania',
            'uk': 'europe', 'germany': 'europe', 'france': 'europe',
            'brazil': 'south_america',
            'canada': 'north_america', 'usa': 'north_america'
        }
        df['region'] = df['country'].map(region_map).fillna('other')
              
        # 8. Drop duplikat & handle null
        df.drop_duplicates(inplace=True)
        
        # Handle null values
        for col in df.columns:
            if df[col].isnull().sum() > 0:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('unknown')
                else:
                    df[col] = df[col].fillna(0)
        
        # 9. Melakukan casting tipe data untuk konsistensi dengan mapping Elasticsearch
        df['age'] = df['age'].astype(int)
        df['minutes_streamed_per_day'] = df['minutes_streamed_per_day'].astype(int)
        df['number_of_songs_liked'] = df['number_of_songs_liked'].astype(int)
        df['discover_engagement'] = df['discover_engagement'].astype(float)
        df['repeat_rate'] = df['repeat_rate'].astype(float)
        
        clean_path = '/tmp/P2M3_Yoseph_Radityo_data_clean.csv'
        df.to_csv(clean_path, index=False)
        print(f"[transform] saved clean CSV: {clean_path}")
        return clean_path

    @task()
    def load(clean_path: str):
        '''
        Fungsi ini memuat data bersih ke Elasticsearch dengan konfigurasi optimal.
        
        Parameters:
            clean_path (str): Path/lokasi file CSV bersih dari proses transform
        
        Proses:
        1. Membuat koneksi ke Elasticsearch dengan timeout dan retry
        2. Verifikasi koneksi
        3. Mendefinisikan mapping struktur indeks
        4. Membuat indeks baru jika belum ada
        5. Melakukan bulk insert dengan error handling
        6. Melaporkan statistik proses indexing
        '''
        df = pd.read_csv(clean_path)
        
        # 1. Menambahkan konfigurasi timeout dan retry
        es = Elasticsearch(
            ['http://elasticsearch:9200'],
            timeout=30,  # Timeout 30 detik
            max_retries=3,  # Maksimal 3 kali retry
            retry_on_timeout=True  # Retry jika timeout
        )
        
        # 2. Melakukan verifikasi koneksi ke Elasticsearch
        if not es.ping():
            raise ValueError("Koneksi ke Elasticsearch gagal!")
        else:
            print("Berhasil terhubung ke Elasticsearch")
        
        index_name = "music_streaming_data"
        
        # 3. Mendefinisikan mapping untuk indeks
        mapping = {
            "mappings": {
                "properties": {
                    "age": {"type": "integer"},
                    "country": {"type": "keyword"},
                    "streaming_platform": {"type": "keyword"},
                    "top_genre": {"type": "keyword"},
                    "minutes_streamed_per_day": {"type": "integer"},
                    "number_of_songs_liked": {"type": "integer"},
                    "most_played_artist": {"type": "keyword"},
                    "subscription_type": {"type": "keyword"},
                    "listening_time": {"type": "keyword"},
                    "discover_engagement": {"type": "float"},
                    "repeat_rate": {"type": "float"},
                    "age_group": {"type": "keyword"},
                    "region": {"type": "keyword"}
                }
            }
        }
        
        # 4. Membuat index dengan mapping jika belum ada
        if not es.indices.exists(index=index_name):
            print(f"Index {index_name} belum ada, membuat baru dengan mapping...")
            es.indices.create(index=index_name, body=mapping)
        else:
            print(f"Index {index_name} sudah ada.")
        
        # 5. Menggunakan bulk helper dengan error handling
        actions = [
            {
                "_index": index_name,
                "_id": i+1,
                "_source": row.to_dict()
            }
            for i, row in df.iterrows()
        ]
        
        # 6. Menambahkan error handling untuk bulk insert
        try:
            success, failed = helpers.bulk(es, actions, stats_only=False)
            print(f"Indexed {success} documents. Failed: {failed}")
            
            if failed:
                for error in failed:
                    print(f"Error: {error}")
        except Exception as e:
            print(f"Terjadi error saat melakukan bulk insert: {str(e)}")
            raise

    
    raw_data = extract()
    clean_data = transform(raw_data)
    load_task = load(clean_data)
    
    # flow pengerjaan
    start >> raw_data >> clean_data >> load_task >> end