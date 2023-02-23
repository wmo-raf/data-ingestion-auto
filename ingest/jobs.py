from ingest.config import SETTINGS
from ingest.dustforecast import DustForecastIngest
from ingest.ecmwf_opendata import ECMWFOpenData

dust_forecast = DustForecastIngest(dataset_id="dust_forecast",
                                   output_dir=SETTINGS.get("DUST_FORECAST_DATA_DIR"),
                                   username=SETTINGS.get("DUST_USERNAME"),
                                   password=SETTINGS.get("DUST_PASSWORD"))

ecmwf_forecast = ECMWFOpenData(dataset_id="ecmwf_forecast",
                               output_dir=SETTINGS.get("ECMWF_FORECAST_DATA_DIR"))

# Jobs
jobs = [
    {
        "job": dust_forecast.run,
        "options": {
            'trigger': "interval", "seconds": 30, "max_instances": 1
        }
    },
    {
        "job": ecmwf_forecast.run,
        "options": {
            'trigger': "interval", "seconds": 30, "max_instances": 1
        }
    },
]
