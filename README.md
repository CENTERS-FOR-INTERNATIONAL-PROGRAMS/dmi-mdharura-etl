## Installation

Install setup tools

`pip install -U pip setuptools`

Install dlt via pip

`pip install dlt dagster dagster-webserver`

Install dagster via pip

`pip install dagster dagster-webserver`

Install mongo_postgres dependencies

`pip install -r mongo_postgres/requirements.txt`

Install [uv](https://github.com/astral-sh/uv) - a package manager for python
```console
# On macOS and Linux.
$ curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows.
$ powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# With pip.
$ pip install uv
```

If installed via the standalone installer, uv can update itself to the latest version:

```console
$ uv self update
```

Then create a virtualenv in the root directory of the project by running

`$ uv venv`

Then activate the environment by running

`$ source .venv/bin/activate`

Install the project dependecies by running

`$ cd etl && uv pip install -e ".[dev]"`


Install dbt project dependencies by running

`$ cd mdharura_dbt && dbt deps`

Add .env file with the following
``

Test if dbt can connect to the database by running

`$ source .env && cd etl/mdharura_etl/dbt && dbt debug`