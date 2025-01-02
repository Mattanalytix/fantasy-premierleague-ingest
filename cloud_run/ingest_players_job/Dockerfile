FROM python:3.13

ADD cloud_run/ingest_players_job/main.py .

ADD cloud_run/ingest_players_job/requirements.txt .

ADD fpl_connector/ fpl_connector/

ADD run.py .

RUN pip install -r requirements.txt

CMD ["python", "./main.py"]