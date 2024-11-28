from typing import Any, Dict, List
import dataiku

def get_dataset_list_and_proj_key() -> Tuple[List[Dict[str, Any]], str]:
    default_project_key = dataiku.default_project_key()
    client = dataiku.api_client()
    project = client.get_project(default_project_key)
    return project.list_datasets(), default_project_key

def list_sql_conns_in_current_projects() -> Dict[str, List[Dict[str, str]]]:
    datasets, default_project_key = get_dataset_list_and_proj_key()
    sql_connection_list: List[Dict[str, str]] = []
    for dataset_dict in datasets:
        connection_name: str = f"{dataset_dict['params'].get('connection')} ({dataset_dict.get('type')})"
        if connection_name not in [c["label"] for c  in sql_connection_list]:
            dataset = Dataset(project_key=default_project_key, name=dataset_dict["name"])
            ds_info = dataset.get_location_info()
            if ds_info.get("locationInfoType","") == 'SQL':
                val: str = dataset_dict['params'].get("connection")
                sql_connection_list.append({"value": val, "label": connection_name})
    return {"choices": sql_connection_list}

def do(payload, config, plugin_config, inputs):

    parameter_name = payload["parameterName"]
    client = dataiku.api_client()
    current_project = client.get_default_project()
    if parameter_name == "connection_name":

        return list_sql_conns_in_current_projects()