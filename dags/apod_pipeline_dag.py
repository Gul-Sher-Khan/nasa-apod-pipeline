# dags/apod_pipeline_dag.py

import logging
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define the file paths
AIRFLOW_HOME = "/usr/local/airflow"
CSV_FILE_PATH = Path(AIRFLOW_HOME) / "include/data/apod_data.csv"
DVC_FILE_PATH = Path(str(CSV_FILE_PATH) + ".dvc")

# Define the Postgres table name
POSTGRES_TABLE_NAME = "nasa_apod_data"


@dag(
    dag_id="nasa_apod_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["mlops", "assignment", "capstone"],
)
def nasa_apod_pipeline():
    """
    🎓 MLOps Assignment 3: NASA APOD Data Pipeline
    A complete ETL pipeline to fetch, transform, load, and version NASA APOD data.
    """

    @task
    def step_1_extract_data():
        """
        Step 1: Data Extraction (E)
        """
        logging.info("Step 1: Extracting data from NASA APOD API...")
        api_url = "https://api.nasa.gov/planetary/apod?api_key=dBnWm6jL70J0MjIXZj9H3uCJQXhqWuBrgbMgfS0y"
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            logging.info("Data extraction successful.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            raise

    @task
    def step_2_transform_data(raw_data: dict):
        """
        Step 2: Data Transformation (T)
        """
        logging.info("Step 2: Transforming data...")
        fields_of_interest = ["date", "title", "url", "explanation"]

        transformed_data = {}
        for field in fields_of_interest:
            transformed_data[field] = raw_data.get(field)

        logging.info(
            f"Transformed data: {{'date': {transformed_data['date']}, 'title': {transformed_data['title']}}}"
        )
        return transformed_data

    @task
    def step_3_load_data(cleaned_data: dict):
        """
        Step 3: Data Loading (L)
        """
        logging.info("Step 3: Loading data to Postgres and CSV...")

        # --- Part A: Load to PostgreSQL ---
        # THIS IS THE FIX: We directly use the hook.
        # 'postgres_default' is guaranteed to exist in Astro.
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        # Create the table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_NAME} (
            date DATE PRIMARY KEY,
            title TEXT,
            url TEXT,
            explanation TEXT
        );
        """
        pg_hook.run(create_table_sql)

        # Use an 'UPSERT' (INSERT ... ON CONFLICT)
        insert_sql = f"""
        INSERT INTO {POSTGRES_TABLE_NAME} (date, title, url, explanation)
        VALUES (%(date)s, %(title)s, %(url)s, %(explanation)s)
        ON CONFLICT (date) DO UPDATE SET
            title = EXCLUDED.title,
            url = EXCLUDED.url,
            explanation = EXCLUDED.explanation;
        """
        pg_hook.run(insert_sql, parameters=cleaned_data)
        logging.info("Data successfully loaded to Postgres.")

        # --- Part B: Load to CSV ---
        CSV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([cleaned_data])

        if CSV_FILE_PATH.exists():
            df.to_csv(CSV_FILE_PATH, mode="a", header=False, index=False)
        else:
            df.to_csv(CSV_FILE_PATH, mode="w", header=True, index=False)

        logging.info(f"Data successfully loaded to CSV at {CSV_FILE_PATH}")
        return str(CSV_FILE_PATH)

    @task
    def step_4_version_data_dvc(csv_filepath: str):
        """
        Step 4: Data Versioning (DVC)
        """
        logging.info("Step 4: Versioning data with DVC...")
        try:
            subprocess.run(
                ["dvc", "add", csv_filepath],
                check=True,
                cwd=AIRFLOW_HOME,
                capture_output=True,
                text=True,
            )
            logging.info(f"DVC add successful for {csv_filepath}")
            return str(DVC_FILE_PATH)
        except subprocess.CalledProcessError as e:
            logging.error(f"DVC add command failed: {e.stderr}")
            raise

    @task
    def step_5_version_code_git(dvc_filepath: str):
        """
        Step 5: Code Versioning (Git/GitHub)
        """
        logging.info("Step 5: Versioning code with Git...")

        try:
            github_token = Variable.get("GITHUB_TOKEN")
            github_repo_url = Variable.get("GITHUB_REPO_URL")

            cwd = AIRFLOW_HOME

            # 1. Configure Git
            subprocess.run(
                ["git", "config", "--global", "user.email", "airflow@example.com"],
                check=True,
                cwd=cwd,
            )
            subprocess.run(
                ["git", "config", "--global", "user.name", "Airflow Bot"],
                check=True,
                cwd=cwd,
            )

            # 2. Set remote URL
            auth_repo_url = f"https://{github_token}@{github_repo_url}"
            subprocess.run(
                ["git", "remote", "set-url", "origin", auth_repo_url],
                check=True,
                cwd=cwd,
            )

            # 3. Add the .dvc file
            subprocess.run(["git", "add", dvc_filepath], check=True, cwd=cwd)

            # 4. Commit
            status_result = subprocess.run(
                ["git", "status", "--porcelain"],
                check=True,
                cwd=cwd,
                capture_output=True,
                text=True,
            )
            if dvc_filepath in status_result.stdout:
                commit_message = f"Auto-commit: Update data for {datetime.now().strftime('%Y-%m-%d')}"
                subprocess.run(
                    ["git", "commit", "-m", commit_message], check=True, cwd=cwd
                )
                logging.info("Git commit successful.")
            else:
                logging.info("No data changes to commit.")

            # 5. Push
            subprocess.run(["git", "push", "origin", "main"], check=True, cwd=cwd)
            logging.info("Git push to main branch successful.")

        except subprocess.CalledProcessError as e:
            logging.error(f"Git command failed: {e.stderr}")
            raise
        except Exception as e:
            logging.error(
                f"Failed to get Airflow Variables. Did you set GITHUB_TOKEN and GITHUB_REPO_URL? Error: {e}"
            )
            raise

    # Define the DAG's task flow
    raw_data = step_1_extract_data()
    cleaned_data = step_2_transform_data(raw_data)
    csv_path = step_3_load_data(cleaned_data)
    dvc_path = step_4_version_data_dvc(csv_path)
    step_5_version_code_git(dvc_path)


# Make the DAG visible in the Airflow UI
nasa_apod_pipeline()
