import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from config import SETTINGS
from ingest.jobs import jobs

logging.basicConfig(
    level=SETTINGS.get('logging', {}).get('level'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y%m%d-%H:%M%p',
)

DEBUG = SETTINGS.get("DEBUG")
TASKS_DEV = SETTINGS.get("TASKS_DEV")

if __name__ == '__main__':
    scheduler = BlockingScheduler()

    for job in jobs:
        if job.get("enabled") and not DEBUG:
            scheduler.add_job(job.get("job"), **job.get("options"))
        else:
            job_id = job.get("id")
            if TASKS_DEV and job_id in TASKS_DEV:
                logging.info(f"[MAIN]: Adding job: {job_id}")
                scheduler.add_job(job.get("job"), **job.get("options"))

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()

    except (KeyboardInterrupt, SystemExit):
        pass
