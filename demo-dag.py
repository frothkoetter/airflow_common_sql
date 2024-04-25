from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator
from airflow.providers.common.sql.operators.sql import ( 
	BranchSQLOperator,
	SQLIntervalCheckOperator,
	SQLColumnCheckOperator, 
	SQLThresholdCheckOperator,
	SQLTableCheckOperator, 
	SQLCheckOperator,
	SQLValueCheckOperator,
	SQLExecuteQueryOperator)

# Define custom fetch handler function to process query results
def process_query_results(cursor, **kwargs):
    results = cursor.fetchall()  # Fetch all rows from the cursor
    for row in results:
        # Process each row (e.g., log row data)
        print(f"Row Data: {row}")

default_args = {
    'owner': 'frothkoe',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date':datetime.now(),
}

dag = DAG(
    'demo-dag',
    default_args=default_args, 
    schedule_interval='@daily', 
    catchup=False, 
    is_paused_upon_creation=False
)
#
# Define variables
#
_CONN_ID="cdw-impala"
_TABLE_NAME="airports"
_DB_NAME="airflow"


sql_check_iata_length = """
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
from (
      with validation as (
                    select {{ params.column }} as field
                      from {{ params.db }}.{{ params.table }}  
                         ),
                         validation_errors as (
	   select field from validation
	    where LENGTH(field) != 3
                        )
select *
from validation_errors
) iata_length_test;
"""

check_iata_length = BranchSQLOperator(
    task_id="check-iata-length",
    conn_id=_CONN_ID,
    sql=sql_check_iata_length,
    params={'db': _DB_NAME, 'table': _TABLE_NAME, 'column':'iata'},
    follow_task_ids_if_false=['check-quotation-mark'],
    follow_task_ids_if_true=['clean-iata-length'],
    dag=dag,
)

sql_clean_iata_length = """
delete from {{ params.db }}.{{ params.table }} 
	where LENGTH({{ params.column }} ) != 3;
"""
clean_iata_length = SQLExecuteQueryOperator(
    task_id="clean-iata-length",
    conn_id=_CONN_ID,
    sql=sql_clean_iata_length,
    params={'db': _DB_NAME, 'table': _TABLE_NAME, 'column':'iata'},
    dag=dag,
)

sql_check_quotation_mark = """
select
      count(*) as failures
    from (
with validation as (
	select {{ params.column }} as field
	from {{ params.db }}.{{ params.table }} 
),
validation_errors as (
	select field from validation
	where field rlike('\"')
)
select *
from validation_errors
) quotation_marks_test;
"""

check_quotation_mark = BranchSQLOperator(
    task_id="check-quotation-mark",
    conn_id=_CONN_ID,
    follow_task_ids_if_false=['check-num-rows'],
    follow_task_ids_if_true=['clean-quotation-mark'],
    sql=sql_check_quotation_mark,
    params={'db': _DB_NAME, 'table': _TABLE_NAME, 'column':'airport'},
    dag=dag,
)

sql_clean_quotation_mark = """
update {{ params.db }}.{{ params.table }}
set {{ params.column }}  = regexp_replace( {{ params.column }} ,'"','')
where  {{ params.column }} rlike('"');
"""
clean_quotation_mark = SQLExecuteQueryOperator(
    task_id="clean-quotation-mark",
    conn_id=_CONN_ID,
    sql=sql_clean_quotation_mark,
    params={'db': _DB_NAME, 'table': _TABLE_NAME, 'column':'airport'},
    dag=dag,
)

sql_check_num_rows = """
select count(1) as num_rows from {{ params.db }}.{{ params.table }};
"""

check_num_rows = SQLCheckOperator(
    task_id="check-num-rows",
    conn_id=_CONN_ID,
    sql=sql_check_num_rows,
    params={'db': _DB_NAME, 'table': _TABLE_NAME},
    trigger_rule="none_failed",
    dag=dag,
)

# Define the SQL query to retrieve the value to be checked
sql_value_check = """
select count(1) as num_rows from {{ params.db }}.{{ params.table }};
"""

# Define the SQLValueCheckOperator to perform the value check
value_check = SQLValueCheckOperator(
    task_id='value-check',
    conn_id=_CONN_ID,  # Airflow connection ID for the database
    sql=sql_value_check,
    params={'db': _DB_NAME, 'table': _TABLE_NAME},
    pass_value=3500,  # Expected value threshold
    tolerance=0.2,
    dag=dag,
)

threshold_check = SQLThresholdCheckOperator(
    task_id="threshold-check",
    conn_id=_CONN_ID,
    sql=sql_value_check,
    params={'db': _DB_NAME, 'table': _TABLE_NAME},
    min_threshold=3000,
    max_threshold=4000,
    dag=dag,
    )

sql_create_dataset = """
create database if not exists {{ params.db }};
drop table if exists {{ params.db }}.{{ params.table }}; 
create table {{ params.db }}.{{ params.table }} 
 stored by iceberg TBLPROPERTIES('format-version'='2')
as
 select * from {{ params.source_db }}.{{ params.source_table }};
"""

create_dataset = SQLExecuteQueryOperator(
    task_id="create-dataset",
    conn_id=_CONN_ID,
    sql=sql_create_dataset,
    split_statements=True,
    params={'db': _DB_NAME, 'table': _TABLE_NAME, 'source_db':'airlinedata','source_table':'airports_csv'},
    return_last=False,
    dag=dag,
)

sql_query_sample = """
select * from {{ params.db }}.{{ params.table }} limit 10;
"""

query_sample = SQLExecuteQueryOperator(
    task_id="dataset-query-cdw",
    conn_id=_CONN_ID,
    sql=sql_query_sample,
    params={'db': _DB_NAME, 'table': _TABLE_NAME },
    dag=dag,
    show_return_value_in_logs=True
)

cursor_sample = SQLExecuteQueryOperator(
    task_id="dataset-cursor-cdw",
    conn_id=_CONN_ID,
    sql=sql_query_sample,
    params={'db': _DB_NAME, 'table': _TABLE_NAME },
    dag=dag,
    show_return_value_in_logs=True,
    handler=process_query_results
)

column_check = SQLColumnCheckOperator(
        task_id="column-check",
        dag = dag,
        conn_id=_CONN_ID,
        table=f'{_DB_NAME}.{_TABLE_NAME}',
        column_mapping={
            "iata": {
                "null_check": {"equal_to": 0},
                "unique_check": {"equal_to": 0},
                "distinct_check": {"geq_to": 2},
            },
        },
    )

table_row_count_check = SQLTableCheckOperator(
        task_id="table-row-count-check",
        dag = dag,
        conn_id=_CONN_ID,
        table=f'{_DB_NAME}.{_TABLE_NAME}',
        checks={
            "row_count_check": {"check_statement": "COUNT(*) between 3000 and 4000"   },
        },
    )


create_dataset >> table_row_count_check >>  check_num_rows >> value_check >> threshold_check >> column_check >> check_iata_length >> clean_iata_length >> check_quotation_mark >> clean_quotation_mark >> query_sample >> cursor_sample
