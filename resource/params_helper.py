from typing import Any, Dict, List, Tuple
import dataiku
from dataiku import Dataset

def get_dataset_list_and_proj_key() -> Tuple[List[Dict[str, Any]], str]:
    default_project_key = dataiku.default_project_key()
    client = dataiku.api_client()
    project = client.get_project(default_project_key)
    return project.list_datasets(), default_project_key

def list_sql_conns_in_current_projects() -> Dict[str, List[Dict[str, str]]]:
    try:
        datasets, default_project_key = get_dataset_list_and_proj_key()
        sql_connection_list: List[Dict[str, str]] = []
        for dataset_dict in datasets:
            connection_name: str = f"{dataset_dict['params'].get('connection')} ({dataset_dict.get('type')})"
            if connection_name not in [c["label"] for c  in sql_connection_list]:
                dataset = Dataset(project_key=default_project_key, name=dataset_dict["name"])
                ds_info = dataset.get_location_info()
                #if ds_info.get("locationInfoType","") == 'SQL':
                val: str = dataset_dict['params'].get("connection")
                sql_connection_list.append({"value": val, "label": connection_name})
        if not sql_connection_list:
                sql_connection_list.append({
                        "value": None,
                        "label": "There are no Snowflake connections available in this project"
                    })
        
        return {"choices": sql_connection_list}
    except Exception as e:
        # Handle cases where the user does not have admin privileges
        if "DKUSecurityRuntimeException" in str(e):
            return {"choices": [{"value": None, "label": "Current User does not have credentials for one of the connections to access Snowflake"}]}
        elif "UnauthorizedException" in str(e):
            return {"choices": [{"value": None, "label": " Action forbidden, you are not admin"}]}
        else:
            # For other DSS-related exceptions
            return {"choices": [{"value": None, "label": f"An unexpected error occurred while retrieving connections"}]}

def list_snowflake_conns() -> Dict[str, List[Dict[str, str]]]:
    try:
        client = dataiku.api_client()
        connections = client.list_connections()
    except Exception as e:
        return {"choices": [{"value": None, "label": " Action forbidden, you are not admin"}]}
    # List to store Snowflake connections
    snowflake_connections: List[Dict[str, str]] = []

    for conn in connections:
        try:
            # Get connection info
            connection_info = client.get_connection(conn).get_info()

            # Check if the connection is of type 'Snowflake'
            if connection_info.get('type') == 'Snowflake':
                connection_name = f"{conn} (Snowflake)"
                snowflake_connections.append({
                    "value": conn,
                    "label": connection_name
                })
        except Exception as e:
            pass
    if not snowflake_connections:
            snowflake_connections.append({
                    "value": None,
                    "label": "There are no Snowflake connections available to you on this instance"
                })
    # Return the filtered list of Snowflake connections
    return {"choices": snowflake_connections}

def do(payload, config, plugin_config, inputs):

    parameter_name = payload["parameterName"]
    client = dataiku.api_client()
    current_project = client.get_default_project()
    if parameter_name == "connection_name_instance":
        return list_snowflake_conns()
    elif parameter_name == "connection_name_project":
        return list_sql_conns_in_current_projects()