import os
import yaml
import logging
import json
import shutil
import git
import dataiku
from dataiku.runnables import Runnable, ResultTable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define constants
DBT_PROJECT_PATH = "/tmp/dbt-cloud-snowflake-demo"
LOCAL_REPO_PATH = DBT_PROJECT_PATH
MANIFEST_PATH = f"{DBT_PROJECT_PATH}/target/manifest.json"
os.environ["DBT_PROJECT_DIR"] = DBT_PROJECT_PATH
PROFILES_PATH = os.path.expanduser("~/.dbt/profiles.yml")


class MyRunnable(Runnable):
    """The base interface for a Python runnable."""

    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config

        # Extract and validate configuration
        self.git_repo_url = self.config.get('git_repo_url')
        self.branch_name = self.config.get('branch_name')
        self.connection_name = self.config.get('connection_name')
        self.sf_user = self.config.get('sf_user')

        if not self.git_repo_url or not self.branch_name or not self.connection_name:
            raise ValueError(
                "Missing configuration. Please provide 'git_repo_url', 'branch_name', and 'connection_name'."
            )

        logger.info(
            f"Initialized MyRunnable with git_repo_url: {self.git_repo_url}, "
            f"branch_name: {self.branch_name}, connection_name: {self.connection_name}"
        )

    def delete_file_or_directory(self, path):
        """Delete a file or directory."""
        try:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                logger.info(f"Deleted: {path}")
            else:
                logger.warning(f"Path not found: {path}")
        except Exception as e:
            logger.error(f"Failed to delete {path}: {e}", exc_info=True)
            raise

    def clone_and_update_repo(self):
        """Clone the git repository and checkout the specified branch."""
        try:
            logger.info(f"Cloning repository {self.git_repo_url} into {LOCAL_REPO_PATH}.")
            repo = git.Repo.clone_from(self.git_repo_url, LOCAL_REPO_PATH)
            repo.git.checkout(self.branch_name)
            repo.remotes.origin.pull()
            logger.info("Repository cloned and updated successfully.")
        except Exception as e:
            logger.error(f"Error cloning or updating the repository: {e}", exc_info=True)
            raise

    def run_dbt_command(self, command):
        """Run a dbt CLI command."""
        from dbt.cli.main import dbtRunner
        runner = dbtRunner()
        try:
            logger.info(f"Running dbt command: {command}")
            result = runner.invoke([command])
            if result.success:
                logger.info(f"dbt {command} completed successfully.")
            else:
                logger.error(f"dbt {command} failed with result: {result}.")
                raise RuntimeError(f"dbt {command} execution failed.")
        except Exception as e:
            logger.error(f"Error executing dbt {command}: {e}", exc_info=True)
            raise

    def extract_dbt_snowflake_metadata(self):
        """Extract metadata from the dbt project's manifest.json."""
        try:
            with open(MANIFEST_PATH, 'r') as f:
                manifest = json.load(f)

            metadata = [
                {
                    'database': node['database'],
                    'schema': node['schema'],
                    'table': node['name']
                }
                for node_name, node in manifest.get('nodes', {}).items()
                if node.get('resource_type') == 'model' and
                node.get('config', {}).get('materialized') in ['table', 'view']
            ]
            logger.info(f"Extracted metadata: {metadata}")
            return metadata
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error reading or parsing manifest.json: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during metadata extraction: {e}", exc_info=True)
            raise


    def setup_dbt_profiles(self):
        """Configure and create the profiles.yml file for dbt."""
        try:
            client = dataiku.api_client()
            sf_connection = client.get_connection(self.connection_name)
            connection_info = sf_connection.get_info()
            
            connection_parameters = connection_info.get_params()
            auth_type = connection_parameters.get('authType')
            
            # Set environment variables and auth config based on authentication type
            env_vars = {
                "DBT_SF_ACCOUNT": connection_parameters.get('host').replace('.snowflakecomputing.com', ''),
                "DBT_SF_DATABASE": connection_parameters.get('db'),
                "DBT_SF_WAREHOUSE": connection_parameters.get('warehouse'),
                "DBT_SF_ROLE": connection_parameters.get('role'),
                "DBT_SF_SCHEMA": connection_parameters.get('defaultSchema')
            }
            
            if auth_type == "OAUTH2_APP":
                oauth_cred = connection_info.get_oauth2_credential()
                env_vars.update({
                    "DBT_SF_ACCESS_TOKEN": oauth_cred.get('accessToken'),
                    "DBT_SF_APP_ID": connection_parameters.get('appId'),
                    "DBT_SF_APP_SECRET": connection_parameters.get('appSecret')
                })
                auth_config = {
                    'authenticator': "oauth",
                    'token': "{{ env_var('DBT_SF_ACCESS_TOKEN') }}"
                }
            elif auth_type == "PASSWORD":
                cred = connection_info.get_basic_credential()
                env_vars.update({
                    "DBT_SF_USER": cred.get('user'),
                    "DBT_SF_PASSWORD": cred.get('password')
                })
                auth_config = {
                    'user': "{{ env_var('DBT_SF_USER') }}",
                    'password': "{{ env_var('DBT_SF_PASSWORD') }}",
                    'authenticator': "snowflake"
                }
            else:
                raise ValueError(f"Unsupported authentication type: {auth_type}")
            
            # Set environment variables
            for key, value in env_vars.items():
                os.environ[key] = value
            
            # Build the Snowflake configuration dictionary
            snowflake_config = {
                'snowflake_demo_project': {
                    'target': 'dev',
                    'outputs': {
                        'dev': {
                            'type': 'snowflake',
                            'account': "{{ env_var('DBT_SF_ACCOUNT') }}",
                            'database': "{{ env_var('DBT_SF_DATABASE') }}",
                            'role': "{{ env_var('DBT_SF_ROLE') }}",
                            'warehouse': "{{ env_var('DBT_SF_WAREHOUSE') }}",
                            'schema': "{{ env_var('DBT_SF_SCHEMA') }}",
                            'threads': 1,
                            'client_session_keep_alive': False
                        }
                    }
                }
            }

            # Add user and password or token to config if necessary
            if 'user' in auth_config:
                snowflake_config['snowflake_demo_project']['outputs']['dev']['user'] = auth_config['user']
                snowflake_config['snowflake_demo_project']['outputs']['dev']['password'] = auth_config['password']
            if 'authenticator' in auth_config:
                snowflake_config['snowflake_demo_project']['outputs']['dev']['authenticator'] = auth_config['authenticator']
            if 'token' in auth_config:
                snowflake_config['snowflake_demo_project']['outputs']['dev']['token'] = auth_config['token']
                snowflake_config['snowflake_demo_project']['outputs']['dev']['user'] = self.sf_user


            # Ensure the profiles.yml directory exists and write the config to the file
            profiles_path = os.path.expanduser("~/.dbt/profiles.yml")
            os.makedirs(os.path.dirname(profiles_path), exist_ok=True)
            
            with open(profiles_path, 'w') as file:
                yaml.dump(snowflake_config, file)
            
            logger.info(f"profiles.yml created at {profiles_path}.")
        
        except Exception as e:
            logger.error(f"An error occurred while setting up dbt profiles: {e}")
            raise


    def run(self, progress_callback):
        """Main execution entry point."""
        try:
            self.setup_dbt_profiles()
            self.delete_file_or_directory(LOCAL_REPO_PATH)
            self.clone_and_update_repo()
            self.run_dbt_command('deps')
            self.run_dbt_command('run')
            metadata = self.extract_dbt_snowflake_metadata()
    
            if metadata:
                rt = ResultTable()
                rt.add_column("database", "Database", "STRING")
                rt.add_column("schema", "Schema", "STRING")
                rt.add_column("table", "Table", "STRING")
                for entry in metadata:
                    rt.add_record([entry['database'], entry['schema'], entry['table']])
                return rt

        except Exception as e:
            logger.error("An error occurred during the dbt workflow execution.", exc_info=True)
            raise  # Return non-zero exit code to indicate failure
        finally:
            # Cleanup profiles.yml and unset all environment variables
            env_vars_to_unset = [
                "DBT_PROFILES_DIR",
                "DBT_PROJECT_DIR",
                "DBT_SF_PASSWORD",
                "DBT_SF_USER",
                "DBT_SF_ACCOUNT",
                "DBT_SF_DATABASE",
                "DBT_SF_WAREHOUSE",
                "DBT_SF_ROLE",
                "DBT_SF_SCHEMA",
                "DBT_SF_ACCESS_TOKEN",
                "DBT_SF_APP_ID",
                "DBT_SF_APP_SECRET"
            ]
            try:
                self.delete_file_or_directory(LOCAL_REPO_PATH)
                self.delete_file_or_directory(PROFILES_PATH)

                for var in env_vars_to_unset:
                    if var in os.environ:
                        del os.environ[var]
                        logger.info(f"Unset environment variable: {var}")
            except Exception as cleanup_error:
                logger.error(f"Cleanup failed: {cleanup_error}", exc_info=True)
                
                
              