
```cmd
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

To build this app run the following from the root of the repository:

```cmd
docker compose build
docker push europe-west2-docker.pkg.dev/raw-nonprod-service-k41/test/ingest_players_job
```
