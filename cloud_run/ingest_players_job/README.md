
```cmd
gcloud auth application-default login
gcloud config set project raw-nonprod-service-k41
```

To build this app run the following from the root of the repository:

Set environment variables:

```cmd
export PROJECT=raw-nonprod-service-k41
export REGION=europe-west2
```

```cmd
docker compose build
docker push europe-west2-docker.pkg.dev/raw-nonprod-service-k41/fantasy-premier-league/ingest-players-job
```
