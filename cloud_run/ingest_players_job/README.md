```cmd
gcloud auth configure-docker \
    europe-west2-docker.pkg.dev
```

```cmd
docker build . -t ingest_players_job
docker tag ingest_players_job europe-west2-docker.pkg.dev/raw-nonprod-service-k41/test/ingest_players_job
docker push europe-west2-docker.pkg.dev/raw-nonprod-service-k41/test/ingest_players_job
```
