from ingest.config import SETTINGS
from ingest.dustforecast import DustForecastIngest
from ingest.ecmwf_opendata import ECMWFOpenData

dust_forecast = DustForecastIngest(dataset_id="dust_forecast",
                                   output_dir=SETTINGS.get("DUST_FORECAST_DATA_DIR"),
                                   username=SETTINGS.get("DUST_AEMET_USERNAME"),
                                   password=SETTINGS.get("DUST_AEMET_PASSWORD"))

ecmwf_forecast = ECMWFOpenData(dataset_id="ecmwf_forecast",
                               output_dir=SETTINGS.get("ECMWF_FORECAST_DATA_DIR"))

# Jobs
jobs = [
    {
        "job": dust_forecast.run,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("DUST_FORECAST_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    },
    {
        "job": ecmwf_forecast.run,
        "options": {
            'trigger': "interval", "seconds": int(SETTINGS.get("ECMWF_FORECAST_UPDATE_INTERVAL_SECONDS")),
            "max_instances": 1
        }
    },
]
