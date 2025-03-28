# python-sheets-update

## *nix command line
```shell

sheet-update --config_path configuration.yml

```

## As a library
```python
configuration :dict =  {
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
updater = SnowflakeToGoogleSheet(config=configuration)

```
