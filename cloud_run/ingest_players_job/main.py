import os
import sys
import logging
import json
import requests
import google.cloud.logging

logging.basicConfig(level=logging.INFO)
client = google.cloud.logging.Client()
client.setup_logging()

# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0)


def main():
    logging.info(f"""Starting Task #{str(TASK_INDEX)},
                 Attempt #{str(TASK_ATTEMPT)}...""")
    base = "https://fantasy.premierleague.com/api/element-summary"
    player = int(TASK_INDEX)+1
    endpoint = f"{base}/{str(player)}"
    logging.info("Getting data from endpoint %s", endpoint)
    r = requests.get(f"{base}/{str(player)}")
    data = r.json()
    with open(f'mnt/fpl-bucket/player-{str(player)}.json', 'w') as f:
        json.dump(data, f)
    logging.info(f"Completed Task #{TASK_INDEX}.")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.error("""Task #%s, Attempt #%s failed: %s""",
                      TASK_INDEX, TASK_ATTEMPT, str(err))
        sys.exit(1)  # Retry Job Task by exiting the process
