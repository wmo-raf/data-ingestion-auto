from ingest.chirps_rainfall import ChirpsRainfall
from ingest.config import SETTINGS
from ingest.dustforecast import DustForecastIngest
from ingest.ecmwf_opendata import ECMWFOpenData
from ingest.tamsat_rainfall import TamSatRainfall

dust_forecast = DustForecastIngest(dataset_id="dust_forecast",
                                   output_dir=SETTINGS.get("DUST_FORECAST_DATA_DIR"),
                                   username=SETTINGS.get("DUST_AEMET_USERNAME"),
                                   password=SETTINGS.get("DUST_AEMET_PASSWORD"))

ecmwf_forecast = ECMWFOpenData(dataset_id="ecmwf_forecast",
                               output_dir=SETTINGS.get("ECMWF_FORECAST_DATA_DIR"),
                               vector_db_conn_conn_params=SETTINGS.get("VECTOR_DB_CONN_PARAMS"))

tamsat_rainfall_estimate = TamSatRainfall(dataset_id="tamsat_rainfall",
                                          output_dir=SETTINGS.get("TAMSAT_RAINFALL_DATA_DIR"), )

chirps_rainfall_estimate = ChirpsRainfall(dataset_id="chirps_rainfall",
                                          output_dir=SETTINGS.get("CHIRPS_RAINFALL_DATA_DIR"))

# Jobs
jobs = [
    # {
    #     "job": dust_forecast.task,
    #     "options": {
    #         'trigger': "interval", "seconds": int(SETTINGS.get("DUST_FORECAST_UPDATE_INTERVAL_SECONDS")),
    #         "max_instances": 1
    #     }
    # },
    # {
    #     "job": ecmwf_forecast.task,
    #     "options": {
    #         'trigger': "interval", "seconds": int(SETTINGS.get("ECMWF_FORECAST_UPDATE_INTERVAL_SECONDS")),
    #         "max_instances": 1
    #     }
    # },
    # {
    #     "job": tamsat_rainfall_estimate.task,
    #     "options": {
    #         'trigger': "interval", "seconds": int(SETTINGS.get("TAMSAT_RAINFALL_UPDATE_INTERVAL_SECONDS")),
    #         "max_instances": 1
    #     }
    # },
    {
        "job": chirps_rainfall_estimate.task,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("CHIRPS_RAINFALL_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    },
]
