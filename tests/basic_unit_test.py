import unittest
import tempfile
import yaml
from sheet_update import  load_config, SnowflakeToGoogleSheet

class TestSheetUpdateModule(unittest.TestCase):

    def setUp(self):
        # Create a dummy configuration for testing.
        self.dummy_config = {
            "snowflake": {
                "user": "dummy",
                "password": "dummy",
                "account": "dummy",
                "warehouse": "dummy",
                "database": "dummy",
                "schema": "dummy",
                "role": "dummy",
                "authenticator": "externalbrowser"
            },
            "google_sheets": {
                "credentials_file": "dummy.json"
            },
            "tasks": [
                {
                    "enabled": True,
                    "id": "dummy_sheet_id",
                    "worksheet_name": "TestSheet",
                    "query_file": "dummy_query.sql",
                    "freeze": {"row": 1, "col": 2}
                }
            ]
        }
        # Save dummy configuration to a temporary file.
        self.temp_config = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        yaml.dump(self.dummy_config, self.temp_config)
        self.temp_config.close()

    def test_load_config(self):
        # Test that the load_config function returns the expected dictionary.
        config = load_config(config_path=self.temp_config.name)
        self.assertIn("snowflake", config)
        self.assertIn("google_sheets", config)
        self.assertIn("tasks", config)

    def test_instantiate_class(self):
        # Instantiate the class with the dummy configuration.
        updater = SnowflakeToGoogleSheet(config=self.dummy_config)
        self.assertIsInstance(updater, SnowflakeToGoogleSheet)

    # You can add more tests here, for example, by mocking
    # the Snowflake and Google Sheets connections to test execute_query and update_sheet.

if __name__ == '__main__':
    unittest.main()