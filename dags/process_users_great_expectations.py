import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)
from datetime import datetime

# base_path = Path(__file__).parents[2]
data_dir = "/opt/airflow/data"
ge_root_dir = "/opt/airflow/great_expectations"

example_data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {
            "my_datasource": {
                "name": "my_pandas_dataframe",
                "class_name": "Datasource",
                "execution_engine": {"class_name": "PandasExecutionEngine"},
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["batch_id"],
                    }
                },
            }
        },
        "config_variables_file_path": os.path.join(ge_root_dir, "uncommitted", "config_variables.yml"),
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "expectations"),
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "uncommitted", "validations"),
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "suppress_store_backend_id": True,
                    "base_directory": os.path.join(ge_root_dir, "checkpoints"),
                },
            },
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "checkpoint_store_name": "checkpoint_store",
        "data_docs_sites": {
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "uncommitted", "data_docs", "local_site"),
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
        "anonymous_usage_statistics": {
            "data_context_id": "abcdabcd-1111-2222-3333-abcdabcdabcd",
            "enabled": False,
        },
        "notebooks": None,
        "concurrency": {"enabled": False},
    }
)

example_checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "expectation_suite_name": "taxi.demo",
        "batch_request": None,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_pandas_dataframe",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": "addresses",
                    "batch_identifiers": {"batch_id": "default_identifier"},
                    "runtime_parameters": {"batch_data": pd.DataFrame({"test": [1, 2, 3]})},
                },
            }
        ],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)

# work in progress
with DAG('process_users_great_expectations', start_date=datetime(2022, 2, 1), schedule='@daily', catchup=False) as dag:
    ready = PythonOperator(
        task_id='ready',
        python_callable=lambda: print("I am ready!")
    )

    ge_data_context_root_dir_with_checkpoint_name_pass = GreatExpectationsOperator(
        task_id="ge_data_context_root_dir_with_checkpoint_name_pass",
        data_context_config=example_data_context_config,
        checkpoint_config=example_checkpoint_config,
        # data_context_root_dir='expectations'
        # checkpoint_name="a.pass.chk",
    )

    ready >> ge_data_context_root_dir_with_checkpoint_name_pass
