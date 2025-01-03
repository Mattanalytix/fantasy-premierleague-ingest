import os
import sys
import logging

import google.cloud.logging

from run import run_players_to_storage

logging.basicConfig(level=logging.INFO)
client = google.cloud.logging.Client()
client.setup_logging()

# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0)


def main():
    logging.info(f"""Starting Task #{str(TASK_INDEX)},
                 Attempt #{str(TASK_ATTEMPT)}...""")
    teams = [x+(int(TASK_INDEX)*4) for x in list(range(1, 6))]
    logging.info("Running for teams %s", teams)
    run_players_to_storage(
        teams=teams
    )
    logging.info(f"Completed Task #{TASK_INDEX}.")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.error("""Task #%s, Attempt #%s failed: %s""",
                      TASK_INDEX, TASK_ATTEMPT, str(err))
        sys.exit(1)  # Retry Job Task by exiting the process
