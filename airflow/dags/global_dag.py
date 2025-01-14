
import os
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup


from load_to_dbs_script import *
from process_energy_script import *
from process_opinion_data import *
from get_opinion_script import *


default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
}



def check_german_json_exists():
   
    json_file_path = '/opt/airflow/datasets/german_production.json'
    if os.path.exists(json_file_path):
        return end_ingestion.task_id
    else:
        return fetch_and_save_german_data.task_id
    
def check_opinion_files_exists():
   
    json_file_path = '/opt/airflow/dags/opinion_data/pullpush_files_by_month'
    if os.path.exists(json_file_path):
        return end_ingestion.task_id
    else:
        return create_scrapping_folders.task_id


global_dag =  DAG(
    dag_id='global_dag',
    default_args=default_args_dict,
    description="Pipeline",
    schedule_interval="@daily",
    catchup=False,
)


with TaskGroup("ingestion_pipeline","data ingestion step",dag=global_dag) as ingestion_pipeline:

    start_ingestion = DummyOperator(
        task_id='start_ingestion',
        dag=global_dag,
    )

    copy_folder = BashOperator(
    task_id='copy_folder_task',
    dag=global_dag,
    bash_command="""
        src_dir="/opt/airflow/datasets/pullpush_files_by_month"
        dest_dir="/opt/airflow/dags/opinion_data"
        cp -r "$src_dir" "$dest_dir"
        """
    )

    end_ingestion = DummyOperator(
        task_id='end_ingestion',
        dag=global_dag,
        trigger_rule='all_done',
    )

    create_folder_task = BashOperator(
        task_id="create_folder_task",
        dag=global_dag,
        bash_command="mkdir -p /opt/airflow/dags/energy_data/france/ /opt/airflow/dags/energy_data/germany/",
    )

    flush_tables_task = PythonOperator(
        task_id="flush_tables_task",
        python_callable=clear_all_tables,
        dag=global_dag,
    )

    check_json_task = BranchPythonOperator(
        task_id='check_json_exists',
        dag=global_dag,
        python_callable=check_german_json_exists,
        provide_context=True,  
    )

    check_opinion_task = BranchPythonOperator(
        task_id='check_opinion_files_exists',
        dag=global_dag,
        python_callable=check_opinion_files_exists,
        provide_context=True,  
    )

    create_scrapping_folders = BashOperator(
        task_id='create_scrapping_folders',
        dag=global_dag,
        bash_command='''#!/bin/bash
        mkdir -p /opt/airflow/datasets/pullpush_files_by_month
        folders=(
          "charbon"
          "eolienne"
          "fossile"
          "kernenergie"
          "nucléaire"
          "solarenergie"
          "windkraft"
          "eolien"
          "fossil"
          "hydraulique"
          "kohle"
          "solaire"
          "wasserkraft"
          "windturbine"
        )
        for folder in "${folders[@]}"
        do
          mkdir -p "/opt/airflow/datasets/pullpush_files_by_month/$folder"
        done
        ''',
    )

    scrap_france_opinion_data = PythonOperator(
        task_id="scrape_france_opinion_data",
        python_callable=scrape_subreddit,
        dag=global_dag,
        op_kwargs={
            "subreddit": "ecologie",
            "search_terms": [
                "nucléaire",
                "charbon",
                "hydraulique",
                "fossile",
                "eolien",
                "eolienne",
                "solaire",
            ],
            "start_year": 2018,
            "end_year": 2024,
        },
    )

    scrap_german_opinion_data = PythonOperator(
        task_id="scrap_german_opinion_data",
        python_callable=scrape_subreddit,
        dag=global_dag,
        op_kwargs={
            "subreddit": "de",
            "search_terms": [
                "kernenergie",
                "kohle",
                "wasserkraft",
                "fossil",
                "windkraft",
                "windturbine",
                "solarenergie",
            ],
            "start_year": 2018,
            "end_year": 2024,
        },
    )
    
    fetch_and_save_german_data = PythonOperator(
        task_id="fetch_and_save_german_data",
        python_callable=save_json_to_file,
        dag=global_dag,
        op_kwargs={"api_url": "https://api.energy-charts.info/total_power?country=de&start=157828435&end=1735751635", "json_file": "/opt/airflow/datasets/german_production.json"},
    )

    start_ingestion >> create_folder_task >>  flush_tables_task >> [check_opinion_task, check_json_task]
    check_opinion_task >> [create_scrapping_folders,end_ingestion]
    check_json_task >> [fetch_and_save_german_data,end_ingestion]
    create_scrapping_folders >> scrap_german_opinion_data
    scrap_german_opinion_data >> scrap_france_opinion_data
    scrap_france_opinion_data >> copy_folder
    copy_folder >> end_ingestion
    fetch_and_save_german_data >> end_ingestion
    



with TaskGroup("wrangling_pipeline","data processing step",dag=global_dag) as wrangling_pipeline:

    start_wrangling = DummyOperator(
        task_id='start_wrangling',
        dag=global_dag,
    )

    end_wrangling = DummyOperator(
        task_id='end_wrangling',
        dag=global_dag,
    )

    transform_opinion_to_sentiment = PythonOperator(
        task_id="transform_opinion_to_sentiment",
        python_callable=process_all_files_in_directory,
        dag=global_dag,
        op_kwargs={
            "base_dir": "/opt/airflow/dags/opinion_data/pullpush_files_by_month",
        },
    )
    
    process_france_production_data = PythonOperator(
        task_id="process_france_production_data",
        python_callable=process_france_production_energy_csv,
        dag=global_dag,
        op_kwargs={"input_csv_path": "/opt/airflow/datasets/france_production.csv","output_csv_path": "/opt/airflow/dags/energy_data/france/processed_france_production.csv"},
    )

    process_france_emission_data = PythonOperator(
        task_id="process_france_emission_data",
        python_callable=process_france_emission_energy_csv,
        dag=global_dag,
        op_kwargs={"input_file": "/opt/airflow/datasets/france_emission.csv","output_file": "/opt/airflow/dags/energy_data/france/processed_france_emission.csv"},
    )

    join_france_emission_production_data = PythonOperator(
        task_id="join_france_emission_production_data",
        python_callable=join_france_emission_energy_csv,
        dag=global_dag,
        op_kwargs={"production_csv_path": "/opt/airflow/dags/energy_data/france/processed_france_production.csv","emission_csv_path": "/opt/airflow/dags/energy_data/france/processed_france_emission.csv","output_csv_path": "/opt/airflow/dags/energy_data/france/joined_france_emission_production_data.csv"},
    )

    process_france_emission_production_data = PythonOperator(
        task_id="process_france_emission_production_data",
        python_callable=filter_france_emission_production_energy_csv,
        dag=global_dag,
        op_kwargs={"input_csv_path": "/opt/airflow/dags/energy_data/france/joined_france_emission_production_data.csv","output_csv_path": "/opt/airflow/dags/energy_data/france/processed_france_emission_production_data.csv"},
    )

    process_and_save_german_data = PythonOperator(
        task_id="process_and_save_german_data",
        python_callable=process_german_data,
        dag=global_dag,
        trigger_rule='all_done',
        op_kwargs={"json_data_path": "/opt/airflow/datasets/german_production.json", "output_csv":"/opt/airflow/dags/energy_data/germany/german_production.csv"},
    )

    process_emission_germany_data = PythonOperator(
        task_id="process_emission_germany_data",
        python_callable=process_emission_germany,
        dag=global_dag,
        op_kwargs={"csv_input_file": "/opt/airflow/datasets/german_emissions.csv", "csv_output_file":"/opt/airflow/dags/energy_data/germany/processed_emission_germany.csv"},
    )

    process_emission_germany_data_monthly = PythonOperator(
        task_id="process_emission_germany_data_monthly",
        python_callable=process_emission_germany_monthly,
        dag=global_dag,
        op_kwargs={"csv_input_file": "/opt/airflow/dags/energy_data/germany/processed_emission_germany.csv", "csv_output_file":"/opt/airflow/dags/energy_data/germany/processed_emission_germany_monthly.csv"},
    )

    aggregate_emission_production_germany_data = PythonOperator(
        task_id="aggregate_emission_production_germany_data",
        python_callable=join_production_emission_germany_data,
        dag=global_dag,
        op_kwargs={"csv_emission_file": "/opt/airflow/dags/energy_data/germany/processed_emission_germany_monthly.csv", "csv_production_file":"/opt/airflow/dags/energy_data/germany/german_production.csv","csv_output_file":"/opt/airflow/dags/energy_data/germany/processed_emission_production_germany.csv"},
    )

    process_emission_production_germany_data = PythonOperator(
        task_id="process_emission_production_germany_data",
        python_callable=process_production_emission_germany,
        dag=global_dag,
        op_kwargs={"csv_input_file": "/opt/airflow/dags/energy_data/germany/processed_emission_production_germany.csv", "csv_output_file":"/opt/airflow/dags/energy_data/germany/german_production_emission.csv"},
    )

    aggregate_energy_data = PythonOperator(
        task_id="aggregate_energy_data",
        python_callable=merge_energy_data,
        dag=global_dag,
    )

    enrich_energy_data = PythonOperator(
        task_id="enrich_energy_data",
        python_callable=enrich_emission_data,
        dag=global_dag,
    )

    process_opinion_json = PythonOperator(
        task_id="process_opinion_json",
        python_callable=process_opinion_data,
        dag=global_dag,
        trigger_rule='all_done',
        op_kwargs={"directory_path": "/opt/airflow/dags/opinion_data/pullpush_files_by_month", "output_csv_path":"/opt/airflow/dags/opinion_data/opinions.csv" },
    )

    process_and_aggregate_opinion_data = PythonOperator(
        task_id="process_and_aggregate_opinion_data",
        python_callable=process_and_aggregate_opinion,
        dag=global_dag,
        op_kwargs={"csv_file": "/opt/airflow/dags/opinion_data/opinions.csv", "output_csv_path":"/opt/airflow/dags/opinion_data/opinions_aggregated.csv" },
    )

    add_country_column_to_opinion = PythonOperator(
        task_id="add_country_column_to_opinion",
        python_callable=add_country_column,
        dag=global_dag,
        op_kwargs={"csv_file": "/opt/airflow/dags/opinion_data/opinions_aggregated.csv", "output_csv_path":"/opt/airflow/dags/opinion_data/opinions_with_country.csv" },
    )

    transform_and_rename_opinion_data = PythonOperator(
        task_id="transform_and_rename_opinion_data",
        python_callable=transform_and_rename,
        dag=global_dag,
        op_kwargs={"csv_file": "/opt/airflow/dags/opinion_data/opinions_with_country.csv", "output_csv_path":"/opt/airflow/dags/opinion_data/opinions_to_load.csv" },
    )

    load_to_staging_db_opinion = PythonOperator(
        task_id="load_to_staging_db_opinion",
        python_callable=load_csv_opinion_to_staging_db,
        dag=global_dag,
        op_kwargs={"csv_file": "/opt/airflow/dags/opinion_data/opinions_to_load.csv"},
    )

    load_to_staging_db_france_data = PythonOperator(
        task_id="load_to_staging_db_france_data",
        python_callable=load_csv_france_to_staging_db,
        dag=global_dag,
        op_kwargs={"csv_file": "/opt/airflow/dags/energy_data/france/processed_france_emission_production_data.csv"},
    )

    load_to_staging_db_german_data = PythonOperator(
        task_id="load_to_staging_db_german_data",
        python_callable=load_csv_germany_to_staging_db,
        dag=global_dag,
        op_kwargs={"csv_file": "/opt/airflow/dags/energy_data/germany/german_production_emission.csv"},
    )

    join_energy_and_opinion_data = PythonOperator(
        task_id="join_energy_and_opinion_data",
        python_callable=join_opinion_and_energy_data,
        dag=global_dag,
    )

    start_wrangling >> [process_france_production_data, process_france_emission_data,process_and_save_german_data,process_emission_germany_data,transform_opinion_to_sentiment]
    [process_france_production_data,process_france_emission_data] >> join_france_emission_production_data
    join_france_emission_production_data >> process_france_emission_production_data
    process_france_emission_production_data >> load_to_staging_db_france_data
    transform_opinion_to_sentiment >> process_opinion_json >> process_and_aggregate_opinion_data
    process_opinion_json >> process_and_aggregate_opinion_data
    process_and_aggregate_opinion_data >> add_country_column_to_opinion
    add_country_column_to_opinion >> transform_and_rename_opinion_data
    transform_and_rename_opinion_data >> load_to_staging_db_opinion
    process_emission_germany_data >> process_emission_germany_data_monthly
    [process_and_save_german_data,process_emission_germany_data_monthly] >> aggregate_emission_production_germany_data
    aggregate_emission_production_germany_data >> process_emission_production_germany_data
    process_emission_production_germany_data >> load_to_staging_db_german_data
    [load_to_staging_db_german_data, load_to_staging_db_france_data] >> aggregate_energy_data
    aggregate_energy_data >> enrich_energy_data
    [enrich_energy_data, load_to_staging_db_opinion] >> join_energy_and_opinion_data
    join_energy_and_opinion_data >> end_wrangling


with TaskGroup("production_pipeline","loading in production stage step",dag=global_dag) as production_pipeline:

    start_production = DummyOperator(
        task_id='start_production',
        dag=global_dag,
    )

    end_production = DummyOperator(
        task_id='end_production',
        dag=global_dag,
    )

    load_in_production_db = PythonOperator(
        task_id="load_in_db",
        python_callable=load_energy_data_to_postgres,
        dag=global_dag,
    )

    start_production >> load_in_production_db >> end_production

start_dag = DummyOperator(
        task_id='start_dag',
        dag=global_dag,
    )

end_dag = DummyOperator(
        task_id='end_dag',
        dag=global_dag,
    )

start_dag >> ingestion_pipeline >>  wrangling_pipeline >> production_pipeline >> end_dag 