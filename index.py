import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import chardet


# Fungsi untuk mengunggah CSV ke MySQL dengan penanganan kesalahan
def upload_csv_to_mysql(file_path, table_name, mysql_url):
    try:
        # Deteksi pengkodean file
        with open(file_path, "rb") as f:
            result = chardet.detect(f.read())
        encoding = result["encoding"]

        # Membaca file CSV menggunakan pandas
        df = pd.read_csv(file_path, encoding=encoding)

        # Membuat engine untuk koneksi ke MySQL
        engine = create_engine(mysql_url)

        # Mengunggah DataFrame ke tabel MySQL
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"Data dari {file_path} berhasil diunggah ke tabel {table_name}.")

    except FileNotFoundError:
        print(f"File {file_path} tidak ditemukan.")

    except pd.errors.EmptyDataError:
        print("File CSV kosong.")

    except pd.errors.ParserError:
        print("Kesalahan saat membaca file CSV.")

    except SQLAlchemyError as e:
        print(f"Kesalahan saat mengunggah ke MySQL: {e}")


# Contoh penggunaan fungsi
file_path = "orderan_september_server.csv"  # Ganti dengan path ke file CSV kamu
table_name = "orderan_september_server"  # Ganti dengan nama tabel yang diinginkan
mysql_url = "mysql+pymysql://root:@localhost:3306/db_zerskin"  # Ganti dengan URL koneksi MySQL kamu

upload_csv_to_mysql(file_path, table_name, mysql_url)
