import logging
import os
from datetime import datetime
from pathlib import Path

import requests

from ingest import DataIngest
from ingest.dateutils import get_next_month_date
from ingest.utils import download_file_temp
import rioxarray as rxr

CONFIG = {
    "params": {
        "rainfall_estimate": {
            "desc": "Rainfall Estimate",
            "variables": ["rfe", "rfe_filled"]
        },
        "rainfall_anomaly": {
            "desc": "Rainfall Anomaly",
            "variables": ["rfe", "rfe_filled"]
        }
    },
    "periods": {
        "daily": {
            "enabled": False,
            "start_year": "1983",
            "start_month": "01",
            "start_day": "01",
            "data_file_templates": {
                "rainfall_estimate": "/daily/{YYYY}/{MM}/rfe{YYYY}_{MM}_{dd}.v3.1.nc",
            }
        },
        "pentadal": {
            "enabled": False,
            "start_year": "1983",
            "start_month": "01",
            "start_pentad": "01",
            "data_file_templates": {
                "rainfall_estimate": "/pentadal/{YYYY}/{MM}/rfe{YYYY}_{MM}-pt{P}.v3.1.nc",
                "rainfall_anomaly": "/pentadal-anomalies/{YYYY}/{MM}/rfe{YYYY}_{MM}-pt{P}_anom.v3.1.nc",
            }
        },
        "dekadal": {
            "enabled": False,
            "start_year": "1983",
            "start_month": "01",
            "start_dekad": "1",
            "data_file_templates": {
                "rainfall_estimate": "/dekadal/{YYYY}/{MM}/rfe{YYYY}_{MM}-dk{D}.v3.1.nc",
                "rainfall_anomaly": "/dekadal-anomalies/{YYYY}/{MM}/rfe{YYYY}_{MM}-dk{D}_anom.v3.1.nc",
            }
        },
        "monthly": {
            "enabled": True,
            "start_year": "1983",
            "start_month": "01",
            "data_file_templates": {
                "rainfall_estimate": "/monthly/{YYYY}/{MM}/rfe{YYYY}_{MM}.v3.1.nc",
                "rainfall_anomaly": "/monthly-anomalies/{YYYY}/{MM}/rfe{YYYY}_{MM}_anom.v3.1.nc",
            }
        },
        "seasonal": {
            "enabled": False,
            "start_year": "1983",
            "start_month": "01",
            "data_file_templates": {
                "rainfall_estimate": "/seasonal/{YYYY}/{MM}/rfe{YYYY}_{MM}_seas.v3.1.nc",
                "rainfall_anomaly": "/seasonal-anomalies/{YYYY}/{MM}/rfe{YYYY}_{MM}_seas_anom.v3.1.nc",
            }
        },
    }
}

PARAMS = {

}


class TamSatRainfall(DataIngest):
    def __init__(self, dataset_id, output_dir, cleanup_old_data=False):
        super().__init__(dataset_id=dataset_id, output_dir=output_dir, cleanup_old_data=cleanup_old_data)

        self.params = CONFIG.get("params")
        self.periods = CONFIG.get("periods")
        self.base_data_url = "http://www.tamsat.org.uk/public_data/data/v3.1"

    def run(self, **kwargs):
        for period, period_config in self.periods.items():
            enabled = period_config.get("enabled")
            if enabled and period == "monthly":
                self.run_monthly()

    def run_monthly(self):
        logging.info('[TAMSAT_RAINFALL]: Trying Monthly Data...')
        period_config = self.periods.get("monthly")

        state = self.get_state() or {}
        monthly_last_update = state.get("monthly")

        if monthly_last_update:
            next_date = get_next_month_date(monthly_last_update)
        else:
            next_data_year = period_config.get("start_year")
            next_data_month = period_config.get("start_month")
            next_date = datetime(int(next_data_year), int(next_data_month), 1)

        for param, file_template in period_config.get("data_file_templates", {}).items():
            param_detail = self.params.get(param, {})

            variables = param_detail.get("variables")

            download_file_path = file_template. \
                replace("{YYYY}", f"{next_date.year}"). \
                replace("{MM}", f"{next_date.month:02d}")

            url = f"{self.base_data_url}{download_file_path}"

            logging.info(f'[TAMSAT_RAINFALL]: Downloading {param} Monthly Data with url {url} and date: {next_date}')

            try:
                self.download_and_save_file(url, period="monthly", param=param, variables=variables,
                                            data_date=next_date)
                date_str = next_date.isoformat()
                self.update_state({"monthly": date_str})

                logging.info(f'[TAMSAT_RAINFALL]: Monthly {param} download success for date:{next_date}!')
            except requests.exceptions.HTTPError as e:
                # file not found
                if e.response.status_code == 404:
                    logging.info(
                        f"[TAMSTAT_RAINFALL]: Request data not yet available: {url}, date: {next_date}. Skipping...")
                    return
                else:
                    raise e

    def download_and_save_file(self, url, period, param, variables, data_date):

        data_file = download_file_temp(url, suffix=".nc")

        # open dataset
        ds = rxr.open_rasterio(data_file)

        # write crs
        ds.rio.write_crs("epsg:4326", inplace=True)

        date_str = data_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        for var in variables:
            namespace = f"{period}_{param}_{var}"

            if var in ds.variables:
                # we expect time to be always of length 1 because we requested for only one timestamp
                data_array = ds[var].isel(time=0)

                data_dir = os.path.join(self.output_dir, namespace)
                out_file = os.path.join(data_dir, f"{namespace}_{date_str}.tif")

                # create data dir
                Path(out_file).parent.absolute().mkdir(parents=True, exist_ok=True)

                data_array.rio.to_raster(out_file, driver="COG", compress="DEFLATE")

                ingest_payload = {
                    "namespace": f"-n {namespace}",
                    "path": f"-p {data_dir}",
                    "datatype": "-t tif",
                    "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                }

                logging.info(
                    f"[TAMSTAT_RAINFALL]: Sending ingest command for param: {namespace} and date: {date_str}")
                self.send_ingest_command(ingest_payload)
