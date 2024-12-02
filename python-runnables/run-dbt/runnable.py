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

    def setup_profiles_yml(self, dbt_sf_user, dbt_sf_account, dbt_sf_password):
        """Create the profiles.yml file for dbt configuration."""
        profiles_path = os.path.expanduser("~/.dbt/profiles.yml")
        os.environ["DBT_SF_PASSWORD"] = dbt_sf_password # Set the environment variable directly
        os.environ["DBT_SF_USER"] = dbt_sf_user 
        os.environ["DBT_SF_ACCOUNT"] = dbt_sf_account
        snowflake_config = {
            'snowflake_demo_project': {
                'target': 'dev',
                'outputs': {
                    'dev': {
                        'type': 'snowflake',
                        'account': "{{ env_var('DBT_SF_ACCOUNT') }}",
                        'user':  "{{ env_var('DBT_SF_USER') }}",
                        'password': "{{ env_var('DBT_SF_PASSWORD') }}",
                        'role': 'DATAIKU_ROLE',
                        'database': 'DATAIKU_DATABASE',
                        'warehouse': 'DATAIKU_WAREHOUSE',
                        'schema': 'DATAIKU_SCHEMA',
                        'threads': 1,
                        'client_session_keep_alive': False
                    }
                }
            }
        }

        os.makedirs(os.path.dirname(profiles_path), exist_ok=True)
        with open(profiles_path, 'w') as file:
            yaml.dump(snowflake_config, file)
        logger.info(f"profiles.yml created at {profiles_path}.")

    def run(self, progress_callback):
        """Main execution entry point."""
        try:
            client = dataiku.api_client()
            sf_connection = client.get_connection(self.connection_name)
            cred = sf_connection.get_info().get_basic_credential()

            self.setup_profiles_yml(
                dbt_sf_user=cred.get('user'),
                dbt_sf_account=sf_connection.get_info().get_params().get('host').replace('.snowflakecomputing.com', ''),
                dbt_sf_password=cred.get('password')
            )

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
                "DBT_SF_PASSWORD",
                "DBT_SF_USER",
                "DBT_SF_ACCOUNT",
                "DBT_PROJECT_DIR"
            ]
            try:
                self.delete_file_or_directory(PROFILES_PATH)
                self.delete_file_or_directory(PROFILES_PATH)

                for var in env_vars_to_unset:
                    if var in os.environ:
                        del os.environ[var]
                        logger.info(f"Unset environment variable: {var}")
            except Exception as cleanup_error:
                logger.error(f"Cleanup failed: {cleanup_error}", exc_info=True)