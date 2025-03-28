"""
sheet_update.py

This module contains functions to:
  - Load a configuration from a YAML file.
  - Execute a Snowflake query.
  - Update a Google Sheets worksheet with DataFrame data.
  - Run tasks defined in the configuration.

The caller can import this module and instantiate configuration from a YAML file.
"""

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
import snowflake.connector
import pandas as pd
import yaml


def load_config(config_path: str = "configuration.yaml") -> dict:
    """
    Load configuration from a YAML file.

    :param config_path: Path to the YAML configuration file.
    :return: Configuration dictionary.
    """
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class SnowflakeToGoogleSheet:
    def __init__(self, **kwargs):
        """
        Initialize with a configuration dictionary or a YAML config path.
        """
        self.config: dict = kwargs.get('config')
        config_path = kwargs.get('config_path')
        if self.config is None and config_path:
            self.config = load_config(config_path=config_path)
        if self.config is None:
            self.config = load_config()  # rely on default config location
        if self.config is None:
            raise ValueError(
                'Either a config dict or a config YAML path must be provided, or there must be a default config file in place.'
            )

        self.sf_params: dict = self.config.get("snowflake")
        if self.sf_params is None:
            raise ValueError('Snowflake configuration is not in config')

        self.gs_params: dict = self.config.get("google_sheets")
        if self.gs_params is None:
            raise ValueError('Google Sheets configuration is not in config')

        self.gs_credentials_file: str = self.gs_params.get("credentials_file")
        if self.gs_credentials_file is None:
            raise ValueError('Google API credentials are not in config')

        self.tasks: list = self.config.get("tasks")

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a Snowflake query and return the result as a pandas DataFrame.

        :param query: SQL query string.
        :return: DataFrame containing query results.
        """
        with snowflake.connector.connect(**self.sf_params) as conn:
            return pd.read_sql(query, con=conn, coerce_float=True, parse_dates=True)

    def update_sheet(self, sheet_id, worksheet_name, dataframe, freeze):
        """
        Update a Google Sheets worksheet with replacement data by first clearing
        the sheet and then writing the DataFrame.

        :param sheet_id: Google Sheets document ID.
        :param worksheet_name: Target worksheet name.
        :param dataframe: DataFrame to write into the worksheet.
        :param freeze: Dictionary with freeze configuration (e.g., {"row": 1, "col": 2}).
        """
        creds = service_account.Credentials.from_service_account_file(
            self.gs_credentials_file,
            scopes=[
                'https://www.googleapis.com/auth/documents.readonly',
                'https://mail.google.com/',
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/presentations'
            ]
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
        set_with_dataframe(worksheet, dataframe)
        worksheet.freeze(freeze.get("row", 1), freeze.get("col", 2))
        print(f"✅ Data successfully written to {worksheet_name} sheet.")

    def run_tasks(self):
        """
        Run the tasks defined in the configuration. For each enabled task, load the corresponding
        SQL query, execute it on Snowflake, and update the designated Google Sheets worksheet with the result.
        """
        for task in self.tasks:
            if task.get("enabled", False):
                workbook_id = task.get("workbook_id")
                if workbook_id is None:
                    raise ValueError("Enabled task must provide a Google workbook ID.")

                worksheet_name = task.get("worksheet_name")
                if worksheet_name is None:
                    raise ValueError("Enabled task must provide a Google worksheet name.")

                query_file = task.get("query_file")
                if query_file is None:
                    raise ValueError("Enabled task must provide a Snowflake query SQL file path.")

                freeze_instructions = task.get("freeze", {})

                # Load SQL query from file.
                with open(query_file, "r") as f:
                    query = f.read()
                if not query:
                    raise ValueError("Query file is empty.")

                # Execute the query.
                df = self.execute_query(query)
                # Clean up column names.
                df.columns = df.columns.str.replace("'", "", regex=False)

                # Write results to the target worksheet.
                self.update_sheet(
                    sheet_id=workbook_id,
                    worksheet_name=worksheet_name,
                    dataframe=df,
                    freeze=freeze_instructions
                )

        print("✅ All tasks completed successfully.")


def main(config_path="configuration.yaml"):
    """
    Main function for running tasks.
      1. Loads configuration from a YAML file.
      2. Instantiates the SnowflakeToGoogleSheet class.
      3. Runs tasks based on the configuration.

    :param config_path: Path to the YAML configuration file.
    """
    config = None
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    updater = SnowflakeToGoogleSheet(config=config)
    updater.run_tasks()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Run sheet update tasks based on YAML configuration."
    )
    parser.add_argument(
        '--config_path',
        type=str,
        default="configuration.yaml",
        help="Path to the YAML configuration file (default: configuration.yaml)"
    )
    args = parser.parse_args()
    main(args.config_path)
