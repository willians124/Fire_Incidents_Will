from google.cloud import bigquery

def query_data(client, dataset_name, table_name):
    query = f"SELECT * FROM `{dataset_name}.{table_name}`"
    return client.query(query)

DATASET_NAME = "tf_dataset_en_bigquery"
TABLE_NAMES = ["fact_incidents", "dim_time", "dim_district", "dim_battalion"]

client = bigquery.Client()

for table_name in TABLE_NAMES:
    result = query_data(client, DATASET_NAME, table_name)
    for row in result:
        print(dict(row))
