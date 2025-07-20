# Analisis Market Untuk **Memperkuat Posisi YouTube Music Di Market Music Streaming Platform Global**

## Repository Outline

Berikut adalah gambaran singkat dari file-file yang ada di repository saya.

```
1. README.md - Penjelasan gambaran umum project
2. data_ddl.txt - skema database
3. data_raw.csv - Dataset raw yang didapatkan dari Kaggle
4. data_clean.csv - Dataset raw yang sudah dibersihkan dan siap dipakai.
5. DAG.py - Workflow otomasi Airflow
6. DAG_graph.JPG - Hasil eksekusi DAG di Airflow
7. GX.ipynb - Validasi data dengan Great Expectations
8. /images/ (folder) - Hasil Visualisasi Analisa di Kibana beserta insightnya terhadap project ini
```

## Problem Background

Industri streaming musik telah menjadi pendorong utama pertumbuhan pendapatan musik global. Menurut Laporan Musik Global IFPI, pendapatan musik rekaman global meningkat sebesar 10,2% pada tahun 2023 menjadi US$28,6 miliar, menandai tahun kesembilan pertumbuhan berturut-turut. Peningkatan ini terutama didorong oleh pelanggan streaming berbayar, dengan pendapatan streaming berlangganan tumbuh sebesar 11,2% dan menyumbang 48,9% dari pasar global. Untuk pertama kalinya, langganan berbayar melampaui 500 juta, dengan total lebih dari 667 juta pengguna akun berlangganan berbayar di seluruh dunia berdasarkan dari artikel IFPI Global Music Report 2023 ([source](https://www.ifpi.org/ifpi-global-music-report-global-recorded-music-revenues-grew-10-2-in-2023/))

## Project Output

Laporan bisnis berbasis data (Kibana) + infrastruktur data terotomasi (Airflow/Elasticsearch) yang memberikan insight strategis untuk memahami preferensi dan keterlibatan pendengar guna meningkatkan retensi dan kepuasan pengguna, Youtube/Youtube Music dalam industri streaming global dibandingkan platform streaming lain. Hasil dari analisa yang sudah saya buat berada pada folder `Hasil Analisa` yang sudah saya sertakan juga pada repository saya.

## Data

### Analisis Dataset: Global Music Streaming Trends and Listener Insights

**Sumber Data**:
Dataset diambil dari: [https://www.kaggle.com/datasets/atharvasoundankar/global-music-streaming-trends-and-listener-insights](https://www.kaggle.com/datasets/atharvasoundankar/global-music-streaming-trends-and-listener-insights)

### **Karakteristik Data**:

1. **Dimensi Data**:

   - **5000 baris** (observasi)
   - **12 kolom** (variabel)
2. **Struktur Kolom**:

   - **Numerik** (5 kolom):`Age`, `Minutes Streamed Per Day`, `Number of Songs Liked`, `Discover Weekly Engagement (%)`, `Repeat Song Rate (%)`.
   - **Kategorik** (7 kolom):
     `User_ID`, `Country`, `Streaming Platform`, `Top Genre`, `Most Played Artist`, `Subscription Type`, `Listening Time`.
3. **Missing Values**:**Tidak ada missing values** pada seluruh kolom.
4. **Statistik Deskriptif** (kolom numerik):

   - **`Age`**: Rentang 13-60 tahun (rata-rata 36.7 tahun).
   - **`Minutes Streamed Per Day`**: Rata-rata 309 menit/hari (min. 10, maks. 600 menit).
   - **`Number of Songs Liked`**: Rata-rata 253 lagu (min. 1, maks. 500 lagu).
   - **`Discover Weekly Engagement`**: Rata-rata 50.3% (min. 10.02%, maks. 89.99%).
   - **`Repeat Song Rate`**: Rata-rata 42.4% (min. 5%, maks. 79.99%).

### **Variabel Kategorik Unik**:

1. **`Country`**: 10 negara.
2. **`Streaming Platform`**: 6 platform.
3. **`Top Genre`**: 10 genre musik.
4. **`Subscription Type`**: 2 tipe
5. **`Listening Time`**: 3 kategori
6. **`Most Played Artist`**: 10 artis

## Method

Di proyek ini, saya membuat pipeline ETL batch otomatis pakai Apache Airflow untuk memindahkan data streaming musik dari PostgreSQL ke Elasticsearch. Sebelum dipakai, data saya validasi dengan Great Expectations (7 expectation, termasuk custom rules) dan saya transform—mulai dari normalisasi kolom sampai penanganan nilai hilang. Hasilnya, saya membuat dashboard analitik interaktif di Kibana yang fokus ke preferensi dan retensi pengguna. Semua itu ditujukan untuk kasih rekomendasi strategis ke Tim Analisis Pelanggan Youtube/YouTube Music. Berikut adalah [hasil analisa](https://github.com/Pesoyyy/Strategic-Market-Analysis-for-YouTube-Music-Global-Positioning/tree/main/Hasil%20analisa) yang sudah saya buat dengan Kibana.

## Reference

* [Dataset from Kaggle](https://www.kaggle.com/datasets/atharvasoundankar/global-music-streaming-trends-and-listener-insights)
* [IFPI Global Music Report](https://www.ifpi.org/ifpi-global-music-report-global-recorded-music-revenues-grew-10-2-in-2023/)
* [TikTok Music Report 2023](https://newsroom.tiktok.com/en-us/tiktok-music-evolution-report)
* [Choco-Up](https://choco-up.com/blog/short-form-video-gen-z-tiktok)
* [Jurnal Bisnis Musik 2022](https://www.musicbusinessjournal.com/article/the-rise-of-algorithmic-curation)
* [Medium](https://medium.com/@TrevorW49/the-rise-of-music-discovery-beyond-algorithms)
* [Spotify](https://www.spotify.com/id-id/student/)
