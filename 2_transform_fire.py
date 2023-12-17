from google.cloud import storage, bigquery
import json
from datetime import datetime

def load_from_gcs(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return json.loads(blob.download_as_string())

def create_bigquery_table(dataset_name, table_name, schema):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return table

def transform_and_load_to_bigquery(raw_data, dataset_name):
    client = bigquery.Client()
    schema_fact_incidents = [
        bigquery.SchemaField("incident_number", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("incident_date", "TIMESTAMP"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("zipcode", "STRING"),
        bigquery.SchemaField("battalion", "STRING"),
        bigquery.SchemaField("station_area", "STRING")
    ]
    schema_dim_time = [
        bigquery.SchemaField("incident_date", "TIMESTAMP"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("day", "INTEGER")
    ]
    schema_dim_district = [
        bigquery.SchemaField("zipcode", "STRING"),
        bigquery.SchemaField("neighborhood_district", "STRING")
    ]
    schema_dim_battalion = [
        bigquery.SchemaField("battalion", "STRING"),
        bigquery.SchemaField("station_area", "STRING")
    ]

    # Crear tablas
    create_bigquery_table(dataset_name, "fact_incidents", schema_fact_incidents)
    create_bigquery_table(dataset_name, "dim_time", schema_dim_time)
    create_bigquery_table(dataset_name, "dim_district", schema_dim_district)
    create_bigquery_table(dataset_name, "dim_battalion", schema_dim_battalion)

    # Transformar y cargar datos
    for record in raw_data:
        incident_date = datetime.fromisoformat(record["incident_date"])
        # Fact Table
        fact_row = {
            "incident_number": record["incident_number"],
            "address": record["address"],
            "incident_date": incident_date,
            "city": record["city"],
            "zipcode": record["zipcode"],
            "battalion": record["battalion"],
            "station_area": record["station_area"]
        }
        client.insert_rows_json(client.dataset(dataset_name).table("fact_incidents"), [fact_row])
        # Dimension Tables
        time_row = {
            "incident_date": incident_date,
            "year": incident_date.year,
            "month": incident_date.month,
            "day": incident_date.day
        }
        district_row = {
            "zipcode": record["zipcode"],
            "neighborhood_district": record.get("neighborhood_district")
        }
        battalion_row = {
            "battalion": record["battalion"],
            "station_area": record["station_area"]
        }
        client.insert_rows_json(client.dataset(dataset_name).table("dim_time"), [time_row])
        client.insert_rows_json(client.dataset(dataset_name).table("dim_district"), [district_row])
        client.insert_rows_json(client.dataset(dataset_name).table("dim_battalion"), [battalion_row])

BUCKET_NAME = "tf-bucket-en-gcp"
SOURCE_BLOB_NAME = "datos_incendios/raw_data.json"
DATASET_NAME = "tf_dataset_en_bigquery"

raw_data = load_from_gcs(BUCKET_NAME, SOURCE_BLOB_NAME)
transform_and_load_to_bigquery(raw_data, DATASET_NAME)
