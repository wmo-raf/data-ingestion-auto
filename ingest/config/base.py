import logging
import os

log_level = logging.getLevelName(os.getenv('LOG', "INFO"))

SETTINGS = {
    'logging': {
        'level': log_level
    },
    'DATASET_STATE_FILE': os.getenv('DATASET_STATE_FILE'),
    'DUST_USERNAME': os.getenv('DUST_USERNAME'),
    'DUST_PASSWORD': os.getenv('DUST_PASSWORD'),
    "DUST_FORECAST_DATA_DIR": os.getenv('DUST_FORECAST_DATA_DIR'),
    "ECMWF_FORECAST_DATA_DIR": os.getenv('ECMWF_FORECAST_DATA_DIR'),
    "AFRICA_SHP_PATH": os.getenv('AFRICA_SHP_PATH'),
    "REQUESTS_TIMEOUT": os.getenv('AFRICA_SHP_PATH', 300)
}
