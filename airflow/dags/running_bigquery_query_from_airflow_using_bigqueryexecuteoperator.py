"""
### BigQueryExecuteQueryOperator Showcase

A DAG shows an example of how to use BigQueryExecuteQueryOperator
"""
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils import timezone


default_args = {
    "owner": "dataength",
    "start_date": timezone.datetime(2021, 7, 30),
}


@dag(default_args=default_args, schedule_interval="*/30 * * * *", catchup=False, tags=["dataength"])
def running_bigquery_query_from_airflow_using_bigqueryexecuteoperator():
    start = DummyOperator(task_id="start")

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

    end = DummyOperator(task_id="end")

    start >> find_top_five_coffee_brands >> end


dag = running_bigquery_query_from_airflow_using_bigqueryexecuteoperator()
dag.doc_md = __doc__
