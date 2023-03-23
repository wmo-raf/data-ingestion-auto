import gzip
import logging
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import requests
import rioxarray as rxr
import xarray as xr

from ingest import DataIngest
from ingest.dateutils import get_next_month_date
from ingest.utils import download_file_temp

CONFIG = {
    "periods": {
        "monthly": {
            "enabled": True,
            "start_year": "1981",
            "start_month": "01",
            "start_day": "01",
            "calculate_anomalies": True,
            "climatology_period": [1981, 2011],
            "file_template": "/africa_monthly/tifs/chirps-v2.0.{YYYY}.{MM}.tif.gz",
        }
    }
}


class ChirpsRainfall(DataIngest):
    def __init__(self, dataset_id, output_dir, cleanup_old_data=False):
        super().__init__(dataset_id=dataset_id, output_dir=output_dir, cleanup_old_data=cleanup_old_data)

        self.periods = CONFIG.get("periods")
        self.base_data_url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0"

    def run(self, **kwargs):
        for period, period_config in self.periods.items():
            enabled = period_config.get("enabled")
            if enabled and period == "monthly":
                self.run_monthly()

    def run_monthly(self):
        logging.info('[CHIRPS_RAINFALL]: Trying Monthly Data...')
        monthly_config = self.periods.get("monthly")

        calculate_anomalies = monthly_config.get("calculate_anomalies")
        climatology_period = monthly_config.get("climatology_period")

        state = self.get_state() or {}
        monthly_last_update = state.get("monthly")

        if monthly_last_update:
            next_date = get_next_month_date(monthly_last_update)
        else:
            next_data_year = monthly_config.get("start_year")
            next_data_month = monthly_config.get("start_month")
            next_date = datetime(int(next_data_year), int(next_data_month), 1)

        file_template = monthly_config.get("file_template")

        next_date_month = f"{next_date.month:02d}"

        download_file_path = file_template. \
            replace("{YYYY}", f"{next_date.year}"). \
            replace("{MM}", next_date_month)

        url = f"{self.base_data_url}{download_file_path}"

        logging.info(f'[CHIRPS_RAINFALL]: Downloading Chirps Monthly Data with url: {url} and date: {next_date}')

        try:

            current_data_file = self.download_and_save_file(url, period="monthly", param="chirps_rainfall_estimate",
                                                            data_date=next_date)
            if calculate_anomalies:
                normal_file = self.get_month_normal(next_date_month, climatology_period, file_template)

                nodata_value = -9999

                data_array_current = rxr.open_rasterio(current_data_file)
                data_array_current = data_array_current.rio.write_nodata(nodata_value, encoded=True)

                data_array_normal = rxr.open_rasterio(normal_file)
                data_array_normal = data_array_normal.rio.write_nodata(nodata_value, encoded=True)

                mask_da1 = data_array_current != nodata_value
                mask_da2 = data_array_normal != nodata_value

                # calculate anomaly
                data_array_anomaly = xr.where(mask_da1 & mask_da2, data_array_current - data_array_normal, nodata_value)

                date_str = next_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                namespace = f"monthly_chirps_rainfall_anomaly"

                data_dir = os.path.join(self.output_dir, namespace)
                out_file = os.path.join(data_dir, f"{namespace}_{date_str}.tif")

                Path(out_file).parent.absolute().mkdir(parents=True, exist_ok=True)

                data_array_anomaly = data_array_anomaly.rio.write_nodata(-9999, encoded=True)
                data_array_anomaly.rio.to_raster(out_file, driver="COG", compress="DEFLATE")

                ingest_payload = {
                    "namespace": f"-n {namespace}",
                    "path": f"-p {data_dir}",
                    "datatype": "-t tif",
                    "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                }

                logging.info(
                    f"[CHIRPS_RAINFALL]: Sending ingest command for period: monthly  param: {namespace} and date: {date_str}")
                self.send_ingest_command(ingest_payload)
        except requests.exceptions.HTTPError as e:
            # file not found
            if e.response.status_code == 404:
                logging.info(
                    f"[CHIRPS_RAINFALL]: Request data not yet available: {url}, date: {next_date}. Skipping...")
                return
            else:
                raise e
        # update state
        self.update_state({"monthly": next_date.isoformat()})

    # get climatological mean for a given month and period [start_year, end_year]
    def get_month_normal(self, month, climatology_period, file_template):
        state = self.get_state() or {}
        monthly_normals = state.get("monthly_normals", {})
        normal = monthly_normals.get(month)

        if normal and os.path.exists(normal):
            return normal

        start_year, end_year = climatology_period

        logging.info(f"[CHIRPS_RAINFALL]: Getting Monthly normals for month :{month}")
        with tempfile.TemporaryDirectory() as temp_dir:
            for year in range(start_year, end_year + 1):
                file_path = file_template. \
                    replace("{YYYY}", f"{year}"). \
                    replace("{MM}", month)
                url = f"{self.base_data_url}{file_path}"

                file_name = os.path.basename(url)[:-3]
                out_file = os.path.join(temp_dir, file_name)
                logging.info(f"[CHIRPS_RAINFALL]: Downloading data for year: {year} and month : {month}")
                self.download_chirps_tif(url, out_file)

            file_pattern = f"{temp_dir}/*tif"
            logging.info(f"[CHIRPS_RAINFALL]: Combining monthly data for years: {start_year}, {end_year}")
            ds = xr.open_mfdataset(file_pattern, combine='nested', concat_dim='band', engine="rasterio")

            # write crs
            ds.rio.write_crs("epsg:4326", inplace=True)
            mean_ds = ds.mean(dim='band')

            monthly_normals_dir = f"{self.output_dir}/normals_{start_year}_{end_year}/monthly"
            normal_file_out = os.path.join(monthly_normals_dir,
                                           f"chirps_monthly_normal_{month}_{start_year}_{end_year}.tif")
            Path(normal_file_out).parent.absolute().mkdir(parents=True, exist_ok=True)

            data_array = mean_ds["band_data"]
            data_array.rio.write_crs("epsg:4326", inplace=True)
            data_array = data_array.rio.write_nodata(-9999, encoded=True)

            # save mean file
            data_array.rio.to_raster(normal_file_out, driver="COG", compress="DEFLATE")

            # update state
            monthly_normals.update({month: normal_file_out})
            self.update_state({"monthly_normals": monthly_normals})

            return monthly_normals.get(month)

    @staticmethod
    def download_chirps_tif(url, out_file=None):
        gz_tif_file = download_file_temp(url)
        if not out_file:
            out_file = tempfile.NamedTemporaryFile(delete=False, suffix=".tif").name
        with gzip.open(gz_tif_file, "rb") as gz_file:
            with open(out_file, "wb") as output_file:
                shutil.copyfileobj(gz_file, output_file)
        return out_file

    def download_and_save_file(self, url, period, param, data_date):
        date_str = data_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        namespace = f"{period}_{param}"

        data_dir = os.path.join(self.output_dir, namespace)
        out_file = os.path.join(data_dir, f"{namespace}_{date_str}.tif")

        Path(out_file).parent.absolute().mkdir(parents=True, exist_ok=True)

        logging.info(
            f"[CHIRPS_RAINFALL]: Downloading {period} data for param: {namespace} and date: {date_str}")
        tif_file = self.download_chirps_tif(url)

        data_array_current = rxr.open_rasterio(tif_file)
        data_array_current.rio.write_crs("epsg:4326", inplace=True)
        data_array_current = data_array_current.rio.write_nodata(-9999, encoded=True)

        # save raster file
        data_array_current.rio.to_raster(out_file, driver="COG", compress="DEFLATE")

        ingest_payload = {
            "namespace": f"-n {namespace}",
            "path": f"-p {data_dir}",
            "datatype": "-t tif",
            "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
        }

        logging.info(
            f"[CHIRPS_RAINFALL]: Sending ingest command for period: {period} param: {namespace} and date: {date_str}")
        self.send_ingest_command(ingest_payload)

        # cleanup
        os.remove(tif_file)

        return out_file
