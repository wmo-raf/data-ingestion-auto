import os

from dotenv import load_dotenv

load_dotenv()

from . import base, dev, production

SETTINGS = base.SETTINGS

if os.getenv('DEBUG'):
    SETTINGS.update(dev.SETTINGS)
else:
    SETTINGS.update(production.SETTINGS)
