```cmd
gcloud auth configure-docker \
    europe-west2-docker.pkg.dev
```

To build this app run the following from the root of the repository:

```cmd
docker compose build
docker push europe-west2-docker.pkg.dev/raw-nonprod-service-k41/test/ingest_players_job
```
