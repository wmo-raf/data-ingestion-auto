import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from ingest.config import SETTINGS
from ingest.jobs import jobs

logging.basicConfig(
    level=SETTINGS.get('logging', {}).get('level'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y%m%d-%H:%M%p',
)

if __name__ == '__main__':
    scheduler = BlockingScheduler()

    for job in jobs:
        scheduler.add_job(job.get("job"), **job.get("options"))

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
