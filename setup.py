from setuptools import setup, find_packages

setup(
    name="sheet_update",
    version="0.1.0",
    description="Module to update Google Sheets from Snowflake queries based on YAML configuration.",
    author="David Ashirov",
    author_email="david@stellans.io",
    packages=find_packages(),  # Automatically find packages in your directory
    install_requires=[
        "gspread",
        "google-auth",
        "snowflake-connector-python",
        "pandas",
        "pyyaml",
    ],
    entry_points={
        "console_scripts": [
            "sheet-update=sheet_update.sheet_update:main",
        ],
    },
)