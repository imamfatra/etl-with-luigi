import logging
import json
import time
import os
import hashlib
import requests
import luigi
import pandas as pd
import zlib

from help import db_connection
from tqdm import tqdm
from bs4 import BeautifulSoup
from sqlalchemy import text

log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'luigi_info.log'),  # simpan log standar
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger()

class HashMixin(luigi.Task):
    def _get_input_hash_path(self):
        return self.output().path + '.hash'
    
    def _calculate_file_hash(self, path):
        # periksa apakah path ada
        if not os.path.exists(path):
            return None
        
        hasher = hashlib.sha256()
        try:
            with open(path, "rb") as f:
                buffer = f.read()
                hasher.update(buffer)
            return hasher.hexdigest()            
        except Exception as err:
            logger.error(f"Failed to create hash file for path {path}: {err}")
            return None
    
    def load_hash_file(self, path):
        # file_hash_path = self._get_input_hash_path()
        load_file_path = self.output().path + ".hash"
        metadata = {
            'input_file_path': path,
            'hash_file': self._calculate_file_hash(path),
            'timestamp_processed': time.time()
        }
        os.makedirs(os.path.dirname(load_file_path), exist_ok=True)
        with open(load_file_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"Hash file {path} successfully created")
    
    def complete(self):
        # periksa file output utama 
        if not self.output().exists():
            logger.info(f"""The main output '{self.output().path}' does not exist. 
                        The task will be executed.""")
            return False
        
        # periksa apakah input file valid
        if not hasattr(self, "input_file") or not os.path.exists(self.input_file) or \
            os.path.getsize(self.input_file) == 0:
            logger.warning(f"""Parameter 'input_file' not found or input 
                           file is invalid or empty. Path: {self.input_file}""")
            return False
    
        current_hash = self._calculate_file_hash(self.input_file)
        if current_hash is None:
            logger.warning("failed to get hash for input file")
            return False

        # mengambil file hash yang tersimpan
        old_hash_path = self._get_input_hash_path()
        if not os.path.exists(old_hash_path):
            logger.warning(f"Previous input hash file '{old_hash_path}' not found.")
            return False
        
        try:
            with open(old_hash_path, 'r') as f:
                data_json = json.load(f)
                old_hash = data_json.get("hash_file")
        except Exception as err:
            logger.warning(f"Failed to read or parse input hash file '{old_hash_path}'")
            return False
        
        # bandingkan kedua hash
        if current_hash == old_hash:
            logger.info(f"Input hash '{self.input_file}' is the same as the stored one")
            return True
        else:
            logger.warning(f"Input hash '{self.input_file}' has changed")
            return False

class ExtractDatabase(luigi.Task):
    def requires(self):
        pass

    def run(self):
        engine = db_connection.postgres_connection("luigi_dwh")
        query = "SELECT * FROM mall_customers_raw"

        df = pd.read_sql(query, engine)
        df.to_csv(self.output().path, index=False)
        logger.info("Data extracted successfully from the database")

    def output(self):
        return luigi.LocalTarget("data/raw/db_extract_data.csv")


class ExtractScrapeData(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        pages = 10
        all_data = []

        for page in tqdm(range(1, pages+1)):
            ulr = f"https://quotes.toscrape.com/page/{page}/"

            response = requests.get(ulr)
            if response.status_code == 200:
                text_result = response.text

                soutp = BeautifulSoup(text_result, "html.parser")
                all_quotes = soutp.find_all(class_="quote")
                for q in all_quotes:
                    quote = q.find("span", class_="text").text
                    author = q.find("small", class_="author").text
                    get_tags = q.find_all(class_="tag")

                    tags_list = []
                    for t in get_tags:
                        tag = t.text
                        tags_list.append(tag)

                    data = {
                        "quote": quote,
                        "author": author,
                        "tags": tags_list
                    }
                    all_data.append(data)
                time.sleep(2)
            else:
                logger.error(f"Failed to get data on page {page}")

        # load data dalam bentuk json
        with open(self.output().path, "w") as f:
            json.dump(all_data, f, indent=4)

        logger.info(f"Data retrieval completed successfully")

    def output(self):
        return luigi.LocalTarget("data/raw/scrape_extract_data.json")

class TansformDatabasae(HashMixin):
    input_file = "data/raw/db_extract_data.csv"

    def requires(self):
        return ExtractDatabase()

    def run(self):
        df = pd.read_csv(self.input().path)

        # mengganti nama tabel
        RENAME_COLUMNS = {
            'CustomerID': 'customer_id',
            'Genre': 'gender',
            'Age': 'age',
            'Annual_Income_(k$)': 'annual_income',
            'Spending_Score': 'spending_score'
        }

        df = df.rename(columns=RENAME_COLUMNS)

        # mengganti value kolom gender
        maping = {
            "Male": "male",
            "Female": "female"
        }

        df['gender'] = df['gender'].map(maping)

        # menghasup missing values
        df = df.dropna()
        df.to_csv(self.output().path, index=False)
        
        # buat hash file
        self.load_hash_file(self.input_file)

    def output(self):
        return luigi.LocalTarget("data/transformasi/db_data_transform.csv")

class TransformScrapeData(HashMixin, luigi.Task):
    input_file = "data/raw/scrape_extract_data.json"

    def requires(self):
        return ExtractScrapeData()
    
    def generate_crs32_id(self, row):
        combined = row["quote"] + row["author"]
        return format(zlib.crc32(combined.encode()), "08x")

    def run(self):
        df = pd.read_json(self.input().path)

        # hapus karakter unicode
        df["quote"] = df["quote"].str.replace('\u201c', '', regex=False)
        df["quote"] = df["quote"].str.replace('\u201d', '', regex=False)

        # menambahkan kolom id
        df["id"] = df.apply(self.generate_crs32_id, axis=1)

        records = df.to_dict(orient="records")
        with open(self.output().path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4)
        
        # buat hash file
        self.load_hash_file(self.input_file)

        logger.info("Transfom scrape data successfully")

    def output(self):
        return luigi.LocalTarget("data/transformasi/scrape_data_transform.json")

class DBLoadData(HashMixin, luigi.Task):
    input_file = "data/transformasi/db_data_transform.csv"

    def requires(self):
        return TansformDatabasae()

    def run(self):
        engine = db_connection.postgres_connection("luigi_dwh")
        df  = pd.read_csv(self.input().path)

        sql = text("""
            INSERT INTO mall_customers_clean (
                customer_id, gender, age, annual_income, spending_score)
            VALUES (:customer_id, :gender, :age, :annual_income, :spending_score)
            ON CONFLICT (customer_id)
            DO UPDATE SET
                gender = EXCLUDED.gender,
                age = EXCLUDED.age,
                annual_income = EXCLUDED.annual_income,
                spending_score = EXCLUDED.spending_score;
        """)
        
        rows = df.to_dict(orient="records")
        with engine.begin() as conn:
            conn.execute(sql, rows)

        # Check dan hapus file di folder raw
        path_extract = "data/raw/db_extract_data.csv"
        if os.path.exists(path_extract):
            os.remove(path_extract)
            logger.info(f"File '{path_extract}' has been deleted successfully.")
        else:
            logger.warning(f"File '{path_extract}' does not exist.")

        logger.info("Load database successfully")

    def output(self):
        return luigi.LocalTarget("data/load/db_data_load.json")

class ScrapeLoadData(HashMixin, luigi.Task):
    input_file = "data/transformasi/scrape_data_transform.json"

    def requires(self):
        return TransformScrapeData()

    def run(self):
        engine = db_connection.postgres_connection("luigi_dwh")
        sql = text("""
            INSERT INTO quotes (id, quote, author, tags)
                VALUES (:id, :quote, :author, :tags)
                ON CONFLICT (id)
                DO UPDATE SET
                    quote = EXCLUDED.quote,
                    author = EXCLUDED.author,
                    tags = EXCLUDED.tags;
        """)
        
        df = pd.read_json(self.input().path)
        row = df.to_dict(orient="records")
        with engine.begin() as conn:
            conn.execute(sql, row)

        # Check dan hapus file di folder raw
        path_extract = "data/raw/scrape_extract_data.json"
        if os.path.exists(path_extract):
            os.remove(path_extract)
            logger.info(f"File '{path_extract}' has been deleted successfully.")
        else:
            logger.warning(f"File '{path_extract}' does not exist.")

        logger.info("Load scrape successfully")

    def output(self):
        return luigi.LocalTarget("data/load/scrape_data_load.json")

def main():
    # luigi.build([DBLoadData(), ScrapeLoadData()], local_scheduler=True)
    luigi.build(
        [DBLoadData(), ScrapeLoadData()],
        workers=2
    )


if __name__ == "__main__":
    main()
