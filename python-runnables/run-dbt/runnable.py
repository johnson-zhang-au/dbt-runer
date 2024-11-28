# This file is the actual code for the Python runnable run-dbt
from dataiku.runnables import Runnable
import os
import yaml
import logging
import dataiku
import os
import sys
import json
import logging
import git
import shutil


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

dbt_project_path = "/tmp/dbt-cloud-snowflake-demo"

os.environ["DBT_PROJECT_DIR"] = dbt_project_path


# Path to the local git repository
local_repo_path = '/tmp/dbt-cloud-snowflake-demo'
manifest_path = f"{dbt_project_path}/target/manifest.json"

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.git_repo_url = self.config.get('git_repo_url')
        self.branch_name = self.config.get('branch_name')
        self.connection_name = self.config.get('connection_name')
        logger.info(f"git_repo_url: {self.git_repo_url}, branch_name: {self.branch_name}, connection_name: {self.connection_name}")
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None
    def extract_dbt_snowflake_metadata(self, manifest_path):
        """
        Extracts final table name, schema, and database information for a dbt project using Snowflake.

        Args:
            manifest_path (str): Path to the dbt project's manifest.json file.

        Returns:
            list: A list of dictionaries containing database, schema, and table details.
        """
        metadata = []
        try:
            # Load the manifest.json file
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

            #print(json.dumps(manifest, indent=4))

            for node_name, node in manifest.get('nodes', {}).items():
                if node.get('resource_type') == 'model' and node.get('config', {}).get('materialized') in ['table', 'view']:
                    # Extract database, schema, and table information
                    database = node['database']
                    schema = node['schema']
                    table = node['name']

                    metadata.append({
                        'database': database,
                        'schema': schema,
                        'table': table
                    })
                    logger.info(f"Database: {database}, Schema: {schema}, Table: {table}")

            logger.info("Metadata extraction complete.")
        except FileNotFoundError:
            logger.error(f"Error: The file {manifest_path} was not found.")
        except json.JSONDecodeError:
            logger.error(f"Error: The file {manifest_path} is not a valid JSON file.")
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        return metadata

    def delete_profile(self):
        profiles_path = os.path.expanduser("~/.dbt/profiles.yml")
        # Check if the file exists and remove it if it does
        if os.path.exists(profiles_path):
            os.remove(profiles_path)
            logger.info(f"{profiles_path} has been removed.")
        else:
            logger.error(f"{profiles_path} does not exist.")
        

    def delete_local_repo(self):
        """Delete the current local git repository if it exists."""
        if os.path.exists(local_repo_path):
            try:
                logger.info(f"Deleting existing local repository at {local_repo_path}.")
                shutil.rmtree(local_repo_path)  # Delete the entire directory
                logger.info("Existing local repository deleted successfully.")
            except Exception as e:
                logger.error(f"Error deleting local repository: {e}", exc_info=True)
                self.delete_profile()
                self.delete_local_repo()

    def clone_and_update_repo(self):
        """Clone the git repository and pull the latest changes."""
        try:
            logger.info("Cloning the repository from GitHub.")
            # Clone the repository
            repo = git.Repo.clone_from(self.git_repo_url, local_repo_path)
            
            logger.info(f"Checking out the {self.branch_name} branch.")
            repo.git.checkout(self.branch_name)  # Checkout the main branch
            
            logger.info("Pulling the latest changes.")
            repo.remotes.origin.pull()  # Pull the latest changes
            repo.remotes.origin.fetch()  # Fetch the latest changes
            
            logger.info(f"Repository status:\n{repo.git.status()}")
        except Exception as e:
            logger.error(f"Error cloning or updating the repository: {e}", exc_info=True)
            self.delete_profile()
            self.delete_local_repo()

    def run_dbt_deps(self):
        """Run the dbt deps command."""
        from dbt.cli.main import dbtRunner
        args = ['deps']
        
        runner = dbtRunner()
        try:
            logger.info("Running dbt deps to install dependencies.")
            runner.invoke(args)
            logger.info("dbt deps completed successfully.")
        except Exception as e:
            logger.error(f"Error running dbt deps: {e}", exc_info=True)
            self.delete_profile()
            self.delete_local_repo()

    def run_dbt_run(self):
        """Run the dbt run command."""
        from dbt.cli.main import dbtRunner
        args = ['run']
        
        runner = dbtRunner()
        metadata = []
        try:
            logger.info("Running dbt run to execute models.")
            res = runner.invoke(args)
            
            if res.success:
                logger.info("dbt run completed successfully.")
                metadata = self.extract_dbt_snowflake_metadata(manifest_path)
            else:
                logger.warning("dbt run completed unsuccessfully.")
                logger.warning(f"Result: {res}")
                self.delete_profile()
                self.delete_local_repo()
        except Exception as e:
            logger.error(f"Error running dbt run: {e}", exc_info=True)
            self.delete_profile()
            self.delete_local_repo()
        return metadata
    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        try:
            # Access the password from the environment variable 'DBT_SF_PASSWORD'
            logger.info("Attempting to retrieve the password from the Dataiku connection.")
            
            client = dataiku.api_client()
            sf_connection = client.get_connection(self.connection_name)
            cred = sf_connection.get_info().get_basic_credential()
            
            """
            auth_info = client.get_auth_info(with_secrets=True)
            for secret in auth_info["secrets"]:
                if secret["key"] == "dbt_sf_password":
                    dbt_sf_password = secret["value"]
            """
            
            dbt_sf_password = cred.get('password')
            dbt_sf_user = cred.get('user')
            
            if not dbt_sf_password:
                raise ValueError("Environment variable DBT_SF_PASSWORD is not set or is empty.")
                
            os.environ["DBT_SF_PASSWORD"] = dbt_sf_password # Set the environment variable directly
            os.environ["DBT_SF_USER"] = dbt_sf_user 
            dbt_sf_user = os.getenv('DBT_SF_USER')

            # Verify the environment variable value
            if dbt_sf_user:
                print(f"dbt_sf_user is set to: {dbt_sf_user}")
            else:
                print("dbt_sf_user is not set.")

            logger.info("Password retrieved successfully from the Dataiku connection.")

            # Define the path to the profiles.yml file (typically located in the ~/.dbt directory)
            profiles_path = os.path.expanduser("~/.dbt/profiles.yml")

            # Snowflake connection configuration
            profile_name = 'snowflake_demo_project'
            snowflake_config = {
                profile_name: {
                    'target': 'dev',
                    'outputs': {
                        'dev': {
                            'type': 'snowflake',
                            'account': 'zc53318.ap-southeast-2',  # e.g., 'xy12345.snowflakecomputing.com'
                            'user': "{{ env_var('DBT_SF_USER') }}",  # e.g., 'your_user'
                            'password': dbt_sf_password,  # Use the password from environment variables
                            'role': 'DATAIKU_ROLE',  # Optional
                            'database': 'DATAIKU_DATABASE',
                            'warehouse': 'DATAIKU_WAREHOUSE',
                            'schema': 'DATAIKU_SCHEMA',
                            'threads': 1,
                            'client_session_keep_alive': False  # Optional, helps with long-running sessions
                        }
                    }
                }
            }

            logger.info(f"profile is: {snowflake_config}")
            # Create the .dbt directory if it doesn't exist
            os.makedirs(os.path.dirname(profiles_path), exist_ok=True)
            logger.info(f"Ensuring the .dbt directory exists at {os.path.dirname(profiles_path)}.")

            # Write the Snowflake profile configuration to the profiles.yml file
            with open(profiles_path, 'w') as file:
                yaml.dump(snowflake_config, file, default_flow_style=False)

            logger.info(f"profiles.yml has been created at {profiles_path}")
            logger.info(f"contnet:{snowflake_config}")

        except Exception as e:
            logger.error("An error occurred while setting up the profiles.yml file.", exc_info=True)
        
        self.delete_local_repo()  # Delete the existing local repository if it exists
        self.clone_and_update_repo()  # Clone and update the repository
        self.run_dbt_deps()  # Run dbt deps to install dependencies
        self.run_dbt_run()   # Run dbt run to execute the models

        self.delete_local_repo()
        self.delete_profile()
            
    

    