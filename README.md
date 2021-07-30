# Running BigQuery Query from Airflow using BigQueryExecuteOperator

```py
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


find_top_five_coffee_brands = BigQueryExecuteQueryOperator(
    task_id="find_top_five_coffee_brands",
    sql="""
    SELECT
      Brand,
      COUNT(Brand) AS BrandCount
    FROM
      `dataength.personal.me_and_coffee`
    GROUP BY
      Brand
    ORDER BY
      BrandCount DESC
    LIMIT 5
    """,
    destination_dataset_table=f"dataength.personal.top_five_coffee_brands",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="my_bigquery_connection",
    use_legacy_sql=False,
)
```
