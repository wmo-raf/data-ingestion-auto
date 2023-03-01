import logging
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import xarray as xr
import xmltodict

from ingest import DataIngest, ParameterMissing
from ingest.utils import download_file_temp


class DustForecastIngest(DataIngest):
    def __init__(self, dataset_id, output_dir, username, password, timeout=None):
        super().__init__(dataset_id=dataset_id, output_dir=output_dir)

        if not username:
            raise ParameterMissing("username required")

        if not password:
            raise ParameterMissing("password required")

        self.username = username
        self.password = password

        self.timeout = timeout

        self.variables = [
            {"variable": "OD550_DUST", "name": "od550_dust", },
            {"variable": "SCONC_DUST", "name": "sconc_dust", "constant": 1000000000, "operation": "multiply",
             "units": "μgm**3"}
        ]

    def run(self):
        logging.info('[DUST_FORECAST]: Starting Process...')
        dataset_state = self.get_state()
        url = "https://dust.aemet.es/thredds/catalog/restrictedDataRoot/MULTI-MODEL/latest/catalog.xml"

        logging.info(f'[DUST_FORECAST]: Getting  catalog with url: {url}')
        r = requests.get(url, timeout=self.timeout)

        logging.debug(f'[DUST_FORECAST]: Parsing  catalog')
        data = xmltodict.parse(r.text)

        dataset_name = data["catalog"]["dataset"]["dataset"]["@name"]

        data_file_date = dataset_name.split("_")[0]

        data_file_date = datetime.strptime(data_file_date, "%Y%m%d").isoformat()

        logging.info(f"[DUST_FORECAST]: Latest date from remote catalog is: {data_file_date}")

        if dataset_state and dataset_state.get("last_update") and dataset_state.get(
                "last_update") == data_file_date:
            logging.info(f'[DUST_FORECAST]: No Update required. Skipping...')
            return
        else:
            file_url = f"https://dust.aemet.es/thredds/fileServer/restrictedDataRoot/MULTI-MODEL/latest/{dataset_name}"
            logging.info(f"[DUST_FORECAST]: Downloading data from url: {file_url}' for date: {data_file_date}")
            tmp_file = download_file_temp(file_url, auth=(self.username, self.password), timeout=self.timeout)

            logging.debug(f"[DUST_FORECAST]: Forecast downloaded successfully to temp file: {tmp_file}")

            processed = self.process(tmp_file)

            if processed:
                for variable in self.variables:
                    param = variable.get("name")
                    # Send ingest command
                    ingest_payload = {
                        "namespace": f"-n {param}",
                        "path": f"-p {self.output_dir}/{param}",
                        "datatype": "-t tif",
                        "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                    }

                    logging.info(
                        f"[DUST_FORECAST]: Sending ingest command for param: {param} and date {data_file_date}")
                    self.send_ingest_command(ingest_payload)

                self.update_state(data_file_date)

    def process(self, temp_file):
        logging.info(f"[DUST_FORECAST]: Processing data...")

        ds = xr.open_dataset(temp_file)

        ds.rio.write_crs("epsg:4326", inplace=True)

        # ds = self.clip_to_africa(ds)

        for var in self.variables:
            variable = var.get("variable")
            param = var.get("name")
            logging.debug(f"[DUST_FORECAST]: Processing variable: {variable}")
            if variable in ds.variables:
                for i, t in enumerate(ds.time.values):
                    logging.debug(f"[DUST_FORECAST]: Processing date: {t}")
                    data_datetime = pd.to_datetime(str(t))
                    date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    param_t_filename = f"{self.output_dir}/{param}/{param}_{date_str}.tif"

                    # create directory if not exists
                    Path(param_t_filename).parent.absolute().mkdir(parents=True, exist_ok=True)

                    data_array = ds[variable].isel(time=i)
                    nodata_value = data_array.encoding.get('nodata', data_array.encoding.get('_FillValue'))

                    if var.get("constant") and var.get("operation"):
                        operation = var.get("operation")
                        logging.info(f"[DUST_FORECAST]: Performing operation : {operation} on data")
                        # perform operation
                        if operation == "multiply":
                            data_array = data_array * var.get("constant")

                        # check if we have new units
                        if var.get("units"):
                            data_array.attrs["units"] = var.get("units")

                    # check that nodata is not nan
                    if np.isnan(nodata_value):
                        data_array = data_array.rio.write_nodata(-9999, encoded=True)

                    data_array.rio.to_raster(param_t_filename, driver="COG")

        # remove downloaded file
        os.remove(temp_file)

        return True
