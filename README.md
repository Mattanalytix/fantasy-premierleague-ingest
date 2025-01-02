# fantasy-premierleague-ingest
An application for ingesting data from the fantasy premier league API

## Local Development

To use the package locally, authenticate with GCP using OAuth:

```cmd
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

Setup environment for local development:

```
conda create -n ingest_fpl pip
conda activate ingest_fpl
pip install -r requirements.txt
```

Optionally, install the following packages for local development:

```
pip install ipykernel
pip install python-dotenv
pip install pytest
pip install twine
```

## Build the Package

To build the package using `twine` run the following on the command line in the root directory:

To authenticate with the artifact registry for local development you can setup the instructions from the [Configure authentication to Artifact Registry for Python package repositories](https://cloud.google.com/artifact-registry/docs/python/authentication?_gl=1*ywr4py*_ga*ODc2ODEwNjE1LjE3MjUxODk2NDY.*_ga_WH2QY8WWF5*MTczNTgwOTA0MS40Mi4xLjE3MzU4MTI5NTYuNjAuMC4w) tutorial.

```cmd
pip install keyring
pip install keyrings.google-artifactregistry-auth
```

```cmd
gcloud artifacts print-settings python --project=raw-prod-service-k41 \
    --repository=fpl-connector \
    --location=europe-west2
```

Add the printed settings to `.pypirc` and `pip.conf`.

```cmd
python setup.py sdist bdist_wheel # build the distribution
twine check dist/* # check everything has built correctly
python3 -m twine upload --repository-url https://europe-west2-python.pkg.dev/raw-prod-service-k41/fpl-connector dist/*
```

To install the package into an environment run:

```cmd
pip install --index-url https://europe-west2-python.pkg.dev/raw-prod-service-k41/fpl-connector/simple/ fpl_connector
```

The package is managed using artifact registry private packages and the steps to upload the package can be understood using the [Store Python packages in Artifact Registry](https://cloud.google.com/artifact-registry/docs/python/store-python) tutorial. To check the repository has uploaded successfully run the following command:

```cmd
gcloud artifacts packages list --repository=fpl-connector
gcloud artifacts versions list --package=fpl-connector
```