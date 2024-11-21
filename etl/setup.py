from setuptools import find_packages, setup

setup(
    name="mdharura_etl",
    # packages=find_packages(exclude=["mdharura_etl_tests"]),
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dlt",
        "dagster",
        "dagster-webserver",
        "pymongo",
        "pymongoarrow",
        "dagster-dbt",
        "dbt-core",
        "dbt-postgres",
        "dlt[postgres]"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
