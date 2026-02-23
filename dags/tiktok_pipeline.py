from airflow.decorators import dag, task, task_group
from airflow.sdk import Asset
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import os
import re
from pymongo import MongoClient

RAW_FILE = '/opt/airflow/data/tiktok_google_play_reviews.csv'
STAGE1_FILE = '/opt/airflow/data/stage1.csv'
STAGE2_FILE = '/opt/airflow/data/stage2.csv'
FINAL_FILE = '/opt/airflow/data/processed_tiktok.csv'

processed_dataset = Asset(f"file://{FINAL_FILE}")

# =======================================
# DAG 1: The Processing Pipeline
# =======================================
@dag(
    dag_id='1_tiktok_processing',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    description='Senses file, branches, and processes data'
)
def processing_dag():

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=RAW_FILE,
        poke_interval=10,
        timeout=600
    )

    @task.branch
    def check_if_empty():
        if os.path.exists(RAW_FILE) and os.path.getsize(RAW_FILE) > 0:
            return 'data_transformations.replace_nulls'
        return 'file_is_empty_log'

    branch_choice = check_if_empty()

    empty_log = BashOperator(
        task_id='file_is_empty_log',
        bash_command='echo "WARNING: The TikTok data file is completely empty!"'
    )

    @task_group(group_id='data_transformations')
    def transformation_group():
        
        @task
        def replace_nulls():
            df = pd.read_csv(RAW_FILE)
            df.fillna('-', inplace=True)
            df.to_csv(STAGE1_FILE, index=False)
            print("Nulls replaced.")

        @task
        def sort_data():
            df = pd.read_csv(STAGE1_FILE)
            df['at'] = pd.to_datetime(df['at'], errors='coerce')
            df.sort_values(by='at', inplace=True)
            df.to_csv(STAGE2_FILE, index=False)
            print("Data sorted by date.")

        @task(outlets=[processed_dataset])
        def clean_content():
            df = pd.read_csv(STAGE2_FILE)
            df['content'] = df['content'].astype(str).apply(
                lambda x: re.sub(r'[^\w\s\.,!?\'"-]', '', x)
            )
            df.to_csv(FINAL_FILE, index=False)
            print("Emojis removed.")

        replace_nulls() >> sort_data() >> clean_content()

    wait_for_file >> branch_choice >> [empty_log, transformation_group()]

# ====================================
# DAG 2: Data-Aware Mongo Loader
# ====================================
@dag(
    dag_id='2_tiktok_mongo_loader',
    start_date=datetime(2024, 1, 1),
    schedule=[processed_dataset], 
    catchup=False,
    description='Loads processed data into MongoDB automatically'
)
def mongo_loader_dag():
    
    @task
    def load_to_mongo():
        df = pd.read_csv(FINAL_FILE)
        records = df.to_dict(orient='records')
        
        client = MongoClient("mongodb://admin:password@mongo:27017/")
        db = client.tiktok_db
        collection = db.reviews
        
        collection.delete_many({})
        collection.insert_many(records)
        print(f"Successfully loaded {len(records)} records into MongoDB!")

    load_to_mongo()

processing_dag()
mongo_loader_dag()