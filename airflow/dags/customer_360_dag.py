from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

from datetime import datetime
import os

profile_config = ProfileConfig(
    profile_name = "default",
    target_name = "dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={
            "database": "CUSTOMER_360",
            "schema": "RAW"
            # "database": "dbt_db",
            # "schema": "dbt_schema"
        }
    )
    
)# The root inside the container is /usr/local/airflow
# Assuming your dbt folder is inside the 'dags' folder of your Astro project
# DBT_DEMO_PATH = "/usr/local/airflow/dags/dbt/demo_dbt_snowflake"
DBT_DEMO_PATH = "/usr/local/airflow/dags/dbt/customer_360"
DBT_EXEC_PATH = "/usr/local/airflow/dbt_env/bin/dbt"



dbt_snowflake_dag = DbtDag(
    project_config = ProjectConfig(DBT_DEMO_PATH,),
    operator_args = {"install_deps": True},
    profile_config = profile_config,
    execution_config = ExecutionConfig(dbt_executable_path=os.environ.get('DBT_BINARY_PATH', DBT_EXEC_PATH)),
    schedule_interval = "@daily",
    start_date= datetime(2026,4,29),
    catchup = False,
    dag_id="customer_360_dag"

)
