import logging
import os

log_level = logging.getLevelName(os.getenv('LOG', "INFO"))

SETTINGS = {
    'logging': {
        'level': log_level
    },

    # app
    'DATASET_STATE_DIR': os.getenv('DATASET_STATE_DIR'),
    "AFRICA_SHP_PATH": os.getenv('AFRICA_SHP_PATH'),
    "REQUESTS_TIMEOUT": os.getenv('AFRICA_SHP_PATH', 300),

    # GSKY Settings
    'GSKY_INGEST_LAYER_WEBHOOK_URL': os.getenv('GSKY_INGEST_LAYER_WEBHOOK_URL'),
    'GSKY_WEBHOOK_SECRET': os.getenv('GSKY_WEBHOOK_SECRET'),

    # Dust Forecast
    'DUST_AEMET_USERNAME': os.getenv('DUST_AEMET_USERNAME'),
    'DUST_AEMET_PASSWORD': os.getenv('DUST_AEMET_PASSWORD'),
    "DUST_FORECAST_DATA_DIR": os.getenv('DUST_FORECAST_DATA_DIR'),
    "DUST_FORECAST_UPDATE_INTERVAL_SECONDS": os.getenv('DUST_FORECAST_UPDATE_INTERVAL_SECONDS', 1800),

    # ECMWF Open Data
    "ECMWF_FORECAST_DATA_DIR": os.getenv('ECMWF_FORECAST_DATA_DIR'),
    "ECMWF_FORECAST_UPDATE_INTERVAL_SECONDS": os.getenv('ECMWF_FORECAST_UPDATE_INTERVAL_SECONDS', 1800),

    # Database connection for vector data
    "VECTOR_DB_CONN_PARAMS": {
        'database': os.getenv("VECTOR_DB_NAME"),
        'user': os.getenv("VECTOR_DB_USER"),
        'password': os.getenv("VECTOR_DB_PASSWORD"),
        'host': os.getenv("VECTOR_DB_HOST"),
        'port': os.getenv("VECTOR_DB_PORT")
    }
}
