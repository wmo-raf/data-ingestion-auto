import logging
import os
import tempfile
from pathlib import Path

import pandas as pd
import xarray as xr
from ecmwf.opendata import Client

from ingest import DataIngest

PRESSURE_LEVELS = [925, 850, 700, 500, 250]

PRESSURE_LEVELS_PARAMS = [
    {"variable": "d", "name": "divergence", "desc": "Divergence", },
    {"variable": "gh", "name": "geopotential_height", "desc": "Geopotential height"},
    {"variable": "q", "name": "specific_humidity", "desc": "Specific humidity"},
    {"variable": "r", "name": "relative_humidity", "desc": "Relative humidity"},
    {"variable": "t", "name": "temperature", "desc": "Temperature"},
    {"variable": "u", "name": "u_wind", "desc": "U Component of Wind"},
    {"variable": "v", "name": "v_wind", "desc": "V Component of Wind"},
    {"variable": "vo", "name": "vorticity", "desc": "Vorticity (relative)"},
]

SINGLE_LEVEL_PARAMS = [
    {"variable": "2t", "name": "temperature", "desc": "2 metre temperature"},
    {"variable": "tp", "name": "total_precipitation", "desc": "Total Precipitation"},
    {"variable": "msl", "name": "mean_sea_level_pressure", "desc": "Mean Sea Level Pressure"},
    {"variable": "10u", "name": "u_wind", "desc": "10 metre U wind component"},
    {"variable": "10v", "name": "v_wind", "desc": "10 metre V wind component"}
]


class ECMWFOpenData(DataIngest):
    def __init__(self, dataset_id, output_dir, cleanup_old_data=True):
        super().__init__(dataset_id=dataset_id, output_dir=output_dir, cleanup_old_data=cleanup_old_data)

        self.single_level_params = SINGLE_LEVEL_PARAMS
        self.pressure_level_params = PRESSURE_LEVELS_PARAMS
        self.pressure_levels = PRESSURE_LEVELS

        # https://www.ecmwf.int/en/forecasts/datasets/open-data
        # For time 00z: 0 to 144 by 3
        # next 6 Days, 3 hour steps
        self.steps = [i for i in range(0, 145, 3)]

        self.client = Client("ecmwf", beta=True)

    def get_latest_date(self, request):
        return self.client.latest(request)

    def retrieve_surface_data(self):
        request = {
            "stream": "oper",
            "type": "fc",
            "levtype": "sfc",
            "param": [p.get("variable") for p in self.single_level_params],
            "time": 0,
            "step": self.steps
        }

        latest = self.get_latest_date(request)

        dataset_state = self.get_state()

        if not latest:
            return None

        logging.info(f'[ECMWF_FORECAST]: Latest data date from remote is: {latest}')
        latest_str = latest.isoformat()

        if dataset_state and dataset_state.get("last_update") and dataset_state.get("last_update") == latest_str:
            logging.info(f'[ECMWF_FORECAST]: No Update required. Skipping...')
            return

        temp_file = tempfile.NamedTemporaryFile(delete=False)

        # update target file name
        request.update({"target": temp_file.name})

        logging.info(f"[ECMWF_FORECAST]: Downloading forecast data for date: {latest}...")
        self.client.retrieve(request)

        file_prefix = f"{request.get('stream')}_{request.get('type')}"
        level_type = f"{request.get('levtype')}"

        self.process(
            temp_file.name,
            self.single_level_params,
            file_prefix,
            latest_str,
            level_type
        )

        return latest_str

    def retrieve_pressure_levels_data(self):
        request = {
            "stream": "oper",
            "type": "fc",
            "levtype": "pl",
            "levelist": self.pressure_levels,
            "param": [p.get("variable") for p in self.pressure_level_params],
            "time": 0,
            "step": self.steps
        }

        logging.info('[ECMWF_FORECAST]: Checking for latest ecmwf data date...')
        latest = self.get_latest_date(request)
        dataset_state = self.get_state()

        if not latest:
            return None

        logging.info(f'[ECMWF_FORECAST]: Latest data date from remote is: {latest}')
        latest_str = latest.isoformat()

        if dataset_state and dataset_state.get("last_update") and dataset_state.get("last_update") == latest_str:
            logging.info(f'[ECMWF_FORECAST]: No Update required. Skipping...')
            return

        temp_file = tempfile.NamedTemporaryFile(delete=False)

        # update target file name
        request.update({"target": temp_file.name})

        logging.info(f"[ECMWF_FORECAST]: Downloading forecast data for date: {latest}...")
        self.client.retrieve(request)

        file_prefix = f"{request.get('stream')}_{request.get('type')}"
        level_type = f"{request.get('levtype')}"

        self.process(
            temp_file.name,
            self.pressure_level_params,
            file_prefix,
            latest_str,
            level_type,
            pressure_levels=request.get("levelist")
        )

        return latest_str

    def run(self):
        logging.info('[ECMWF_FORECAST]: Starting Process...')

        # get surface data
        self.retrieve_surface_data()

        # get pressure levels data
        latest_str = self.retrieve_pressure_levels_data()

        if latest_str:
            # update state
            self.update_state(latest_str)

    def process(self, file_path, params, file_prefix_id, latest_str, level_type, pressure_levels=()):
        logging.info(f"[ECMWF_FORECAST]: Processing data...")

        # convert to nc
        nc_out_tmp = tempfile.NamedTemporaryFile(delete=False)

        logging.info(f"[ECMWF_FORECAST]: Converting grib to nc ...")
        self.grib_to_netcdf(file_path, nc_out_tmp.name)

        ds = xr.open_dataset(nc_out_tmp.name)

        # write projection info
        ds.rio.write_crs("epsg:4326", inplace=True)

        for p in params:
            data_var = p.get("variable")
            logging.debug(f"[ECMWF_FORECAST]: Processing variable: {data_var}")

            if data_var in ds.variables:
                param = p.get("name") if p.get("name") else p.get("variable")
                file_prefix = f"{file_prefix_id}_{param}_{level_type}"

                # handle pressure levels data
                if len(pressure_levels) and "plev" in ds.variables:
                    for p_index, p_lev in enumerate(ds.plev.values):
                        # convert to hPa
                        p_hpa = int(p_lev / 100)

                        namespace = f"{file_prefix}_{p_hpa}"

                        for time_index, t in enumerate(ds.time.values):
                            data_datetime = pd.to_datetime(str(t))
                            date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

                            param_p_filename = f"{self.output_dir}/{namespace}/{namespace}_{date_str}.tif"
                            # create directory if not exists
                            Path(param_p_filename).parent.absolute().mkdir(parents=True, exist_ok=True)
                            # select data for time and pressure level
                            data_array = ds[data_var].isel(time=time_index, plev=p_index)
                            # save data as geotiff
                            data_array.rio.to_raster(param_p_filename, driver="COG")

                        # cleanup old forecasts before ingestion
                        self.cleanup_old_data(latest_str)
                        # send ingest command
                        ingest_payload = {
                            "namespace": f"-n {namespace}",
                            "path": f"-p {self.output_dir}/{namespace}",
                            "datatype": "-t tif",
                            "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                        }

                        logging.info(
                            f"[ECMWF_FORECAST]: Sending ingest command for namespace: {namespace}")

                        self.send_ingest_command(ingest_payload)
                else:
                    namespace = f"{file_prefix}"
                    for i, t in enumerate(ds.time.values):
                        data_datetime = pd.to_datetime(str(t))
                        date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

                        param_t_filename = f"{self.output_dir}/{namespace}/{namespace}_{date_str}.tif"
                        # create directory if not exists
                        Path(param_t_filename).parent.absolute().mkdir(parents=True, exist_ok=True)

                        data_array = ds[data_var].isel(time=i)
                        # nodata_value = data_array.encoding.get('nodata', data_array.encoding.get('_FillValue'))
                        #
                        # # check that nodata is not nan
                        # if np.isnan(nodata_value):
                        #     data_array = data_array.rio.write_nodata(-9999, encoded=True)

                        # save data as geotiff
                        data_array.rio.to_raster(param_t_filename, driver="COG")

                    # cleanup old forecasts before ingestion
                    self.cleanup_old_data(latest_str)
                    # send ingest command
                    ingest_payload = {
                        "namespace": f"-n {namespace}",
                        "path": f"-p {self.output_dir}/{namespace}",
                        "datatype": "-t tif",
                        "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                    }

                    logging.info(
                        f"[ECMWF_FORECAST]: Sending ingest command for namespace: {namespace}")

                    self.send_ingest_command(ingest_payload)

        # remove downloaded/temp files
        os.remove(file_path)
        os.remove(nc_out_tmp.name)

        return True
