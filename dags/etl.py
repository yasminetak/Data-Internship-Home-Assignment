from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import pandas as pd
import json

def clean_description(description):
    import re
    clean_description = re.sub(r'<.*?>', '', description)
    return clean_description

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""

@task()
def extract():
    """Extract data from jobs.csv."""
    df = pd.read_csv('source/jobs.csv')
    return df['context'].tolist()

@task()
def transform(context_data: list):
    """Clean and convert extracted elements to json."""
    transformed_data = []
    for index, data in enumerate(context_data):
        job_data = json.loads(data)

        transformed_item = {
            "job": {
                "title": job_data.get("title", ""),
                "industry": job_data.get("industry", ""),
                "description": clean_description(job_data.get("description", "")),
                "employment_type": job_data.get("employmentType", ""),
                "date_posted": job_data.get("datePosted", ""),
            },
            "company": {
                "name": job_data.get("hiringOrganization", {}).get("name", ""),
                "link": job_data.get("hiringOrganization", {}).get("sameAs", ""),
            },
            "education": {
                "required_credential": job_data.get("educationRequirements", {}).get("credentialCategory", ""),
            },
            "experience": {
                "months_of_experience": job_data.get("experienceRequirements", {}).get("monthsOfExperience", ""),
                "seniority_level": job_data.get("experienceRequirements", {}).get("seniority_level", ""),
            },
            "salary": {
                "currency": job_data.get("salary", {}).get("currency", ""),
                "min_value": job_data.get("salary", {}).get("min_value", ""),
                "max_value": job_data.get("salary", {}).get("max_value", ""),
                "unit": job_data.get("salary", {}).get("unit", ""),
            },
            "location": {
                "country": job_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                "locality": job_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                "region": job_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                "postal_code": job_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                "street_address": job_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                "latitude": job_data.get("jobLocation", {}).get("latitude", ""),
                "longitude": job_data.get("jobLocation", {}).get("longitude", ""),
            },
        }

        transformed_data.append(transformed_item)

        # Save transformed item to staging/transformed as json file
        save_transformed_data(transformed_item, index)

    return transformed_data


def save_transformed_data(transformed_item, index):
    with open(f'staging/transformed/job_{index}.json', 'w') as file:
        json.dump(transformed_item, file, indent=2)



@task()
def load(transformed_data: list):
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()
    cursor = connection.cursor()

    for index, data in enumerate(transformed_data):
        job_data = data["job"]
        company_data = data["company"]
        education_data = data["education"]
        experience_data = data["experience"]
        salary_data = data["salary"]
        location_data = data["location"]

        # Example SQL INSERT statements (customize based on your table structure)
        cursor.execute("""
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
        """, (
            job_data["title"],
            job_data["industry"],
            job_data["description"],
            job_data["employment_type"],
            job_data["date_posted"],
        ))

        cursor.execute("""
            INSERT INTO company (job_id, name, link)
            VALUES (last_insert_rowid(), ?, ?)
        """, (
            company_data["name"],
            company_data["link"],
        ))

        cursor.execute("""
            INSERT INTO education (job_id, required_credential)
            VALUES (last_insert_rowid(), ?)
        """, (
            education_data["required_credential"],
        ))

        cursor.execute("""
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (last_insert_rowid(), ?, ?)
        """, (
            experience_data["months_of_experience"],
            experience_data["seniority_level"],
        ))

        cursor.execute("""
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES (last_insert_rowid(), ?, ?, ?, ?)
        """, (
            salary_data["currency"],
            salary_data["min_value"],
            salary_data["max_value"],
            salary_data["unit"],
        ))

        cursor.execute("""
            INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES (last_insert_rowid(), ?, ?, ?, ?, ?, ?, ?)
        """, (
            location_data["country"],
            location_data["locality"],
            location_data["region"],
            location_data["postal_code"],
            location_data["street_address"],
            location_data["latitude"],
            location_data["longitude"],
        ))

    connection.commit()
    connection.close()

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extract_data = extract()
    transform_data = transform(extract_data)
    load = load(transform_data)

    create_tables >> extract() >> transform() >> load()

etl_dag()
