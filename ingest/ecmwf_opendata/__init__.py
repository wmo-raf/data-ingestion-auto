import logging
import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from ecmwf.opendata import Client

from ingest import DataIngest


class ECMWFOpenData(DataIngest):
    def __init__(self, dataset_id, output_dir):
        super().__init__(dataset_id=dataset_id, output_dir=output_dir)

        self.params = [
            {"variable": "2t", "name": "t2m"},
            {"variable": "tp"},
            {"variable": "msl"},
            {"variable": "10u"},
            {"variable": "10v"}
        ]

        # https://www.ecmwf.int/en/forecasts/datasets/open-data
        # For time 00z: 0 to 144 by 3
        # next 6 Days, 3 hour steps
        self.steps = [i for i in range(0, 145, 3)]

        self.request = {
            "stream": "oper",
            "type": "fc",
            "param": [p.get("variable") for p in self.params],
            "time": 0,
            "step": self.steps
        }

        self.client = Client("ecmwf", beta=True)

    def run(self):
        logging.info('[ECMWF_FORECAST]: Starting Process...')
        dataset_state = self.get_state()

        logging.info('[ECMWF_FORECAST]: Checking for latest ecmwf data date...')

        latest = self.client.latest(self.request)

        if latest:
            logging.info(f'[ECMWF_FORECAST]: Latest data date from remote is: {latest}')
            latest_str = latest.isoformat()

            if dataset_state and dataset_state.get("last_update") and dataset_state.get("last_update") == latest_str:
                logging.info(f'[ECMWF_FORECAST]: No Update required. Skipping...')
                return

            tmp_file = tempfile.NamedTemporaryFile(delete=False)
            # update target file name
            self.request.update({"target": tmp_file.name})

            logging.info(f"[ECMWF_FORECAST]: Downloading forecast data for date: {latest}...")
            self.client.retrieve(self.request)

            processed = self.process(tmp_file.name)

            if processed:
                # send ingest command
                for p in self.params:
                    param = p.get("name") if p.get("name") else p.get("variable")
                    ingest_payload = {
                        "namespace": f"-n {self.request.get('stream')}_{self.request.get('type')}_{param}",
                        "path": f"-p {self.output_dir}/{param}",
                        "datatype": "-t tif",
                        "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                    }

                    logging.info(
                        f"[ECMWF_FORECAST]: Sending ingest command for param: {param} and date {latest_str}")

                    self.send_ingest_command(ingest_payload)

                self.update_state(latest_str)

    def process(self, temp_file):
        logging.info(f"[ECMWF_FORECAST]: Processing data...")

        # convert to nc
        nc_out_tmp = tempfile.NamedTemporaryFile(delete=False)

        logging.info(f"[ECMWF_FORECAST]: Converting grib to nc ...")
        self.grib_to_netcdf(temp_file, nc_out_tmp.name)

        ds = xr.open_dataset(nc_out_tmp.name)

        # write projection info
        ds.rio.write_crs("epsg:4326", inplace=True)

        for p in self.params:
            data_var = p.get("variable")
            logging.debug(f"[ECMWF_FORECAST]: Processing variable: {data_var}")

            if data_var in ds.variables:
                param = p.get("name") if p.get("name") else p.get("variable")

                for i, t in enumerate(ds.time.values):
                    data_datetime = pd.to_datetime(str(t))
                    date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    file_prefix = f"{self.request.get('stream')}_{self.request.get('type')}"
                    param_t_filename = f"{self.output_dir}/{param}/{file_prefix}_{param}_{date_str}.tif"

                    # create directory if not exists
                    Path(param_t_filename).parent.absolute().mkdir(parents=True, exist_ok=True)

                    data_array = ds[data_var].isel(time=i)
                    # nodata_value = data_array.encoding.get('nodata', data_array.encoding.get('_FillValue'))

                    # # check that nodata is not nan
                    # if np.isnan(nodata_value):
                    #     data_array = data_array.rio.write_nodata(-9999, encoded=True)

                    # save data as geotiff
                    data_array.rio.to_raster(param_t_filename, driver="COG")

        # remove downloaded/temp files
        os.remove(temp_file)
        os.remove(nc_out_tmp.name)

        return True
