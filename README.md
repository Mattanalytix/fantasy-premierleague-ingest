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
```