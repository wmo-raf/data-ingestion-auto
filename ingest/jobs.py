from ingest.cams_forecast import CamsForecast
from ingest.chirps_rainfall import ChirpsRainfall
from config import SETTINGS
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

cams_forecast = CamsForecast(dataset_id="cams_forecast",
                             output_dir=SETTINGS.get("CAMS_FORECAST_DATA_DIR"),
                             api_key=SETTINGS.get("CAMS_API_KEY"))

# Jobs
jobs = [
    {
        "job": dust_forecast.run,
        "id": dust_forecast.dataset_id,
        "enabled": True,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("DUST_FORECAST_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    },
    {
        "job": ecmwf_forecast.run,
        "id": ecmwf_forecast.dataset_id,
        "enabled": True,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("ECMWF_FORECAST_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    },
    {
        "job": tamsat_rainfall_estimate.run,
        "id": tamsat_rainfall_estimate.dataset_id,
        "enabled": True,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("TAMSAT_RAINFALL_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    },
    {
        "job": chirps_rainfall_estimate.run,
        "id": chirps_rainfall_estimate.dataset_id,
        "enabled": True,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("CHIRPS_RAINFALL_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    },
    {
        "job": cams_forecast.run,
        "id": cams_forecast.dataset_id,
        "enabled": True,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("CAMS_FORECAST_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    }
]
