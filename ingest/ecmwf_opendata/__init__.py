import logging
import os
import tempfile
from pathlib import Path

import pandas as pd
import xarray as xr
from ecmwf.opendata import Client

from ingest import DataIngest

SURFACE_LEVEL_PARAMS = [
    {
        "variable": "2t",
        "name": "temperature",
        "desc": "2 metre Temperature",
        "units": "K",
        "convert": {
            "operation": "subtract",
            "constant": 273.15,
            "units": "degC"
        }
    },
    {
        "variable": "tp",
        "name": "total_precipitation",
        "desc": "Total Precipitation",
        "units": "m",
        "convert": {
            "constant": 1000,
            "operation": "multiply",
            "units": "mm"
        }
    },
    {
        "variable": "msl",
        "name": "mean_sea_level_pressure",
        "desc": "Mean Sea Level Pressure",
        "convert": {
            "constant": 100,
            "operation": "divide",
            "units": "hPa"
        },
        "vectors": [
            {
                "type": "contour",
                "options": {
                    "attr_name": "el_val",
                    "interval": 5,
                    "schema": "pgadapter",
                }
            }
        ]
    },
    {"variable": "10u", "name": "u_wind", "desc": "10 metre U wind component", "units": "m s**-1"},
    {"variable": "10v", "name": "v_wind", "desc": "10 metre V wind component", "units": "m s**-1"}
]

# PRESSURE_LEVELS = [1000, 925, 850, 700, 500, 300, 250, 200, 50]
PRESSURE_LEVELS = [1000]

PRESSURE_LEVELS_PARAMS = [
    {
        "variable": "gh",
        "name": "geopotential_height",
        "desc": "Geopotential Height",
        "units": "gpm",
        "convert": {
            "operation": "divide",
            "constant": 10,
            "units": "gpdm",
        },
        "vectors": [
            {
                "type": "contour",
                "options": {
                    "attr_name": "el_val",
                    "interval": 5,
                    "schema": "pgadapter",
                }
            }
        ]
    },
    {
        "variable": "q",
        "name": "specific_humidity",
        "desc": "Specific humidity",
        "units": "kg kg**-1",
        "convert": {
            "operation": "multiply",
            "constant": 100000,
            "units": "g kg**-1",
        }
    },
    {"variable": "r", "name": "relative_humidity", "desc": "Relative humidity", "units": "%"},
    {
        "variable": "t",
        "name": "temperature",
        "desc": "Temperature",
        "units": "K",
        "convert": {
            "operation": "subtract",
            "constant": 273.15,
            "units": "degC"
        }
    },
    {"variable": "u", "name": "u_wind", "desc": "U Component of Wind", "units": "m s**-1"},
    {"variable": "v", "name": "v_wind", "desc": "V Component of Wind", "units": "m s**-1"},
    {
        "variable": "d",
        "name": "divergence",
        "desc": "Divergence",
        "units": "s**-1",
        "convert": {
            "constant": 100000,
            "operation": "multiply",
            "units": "10**-5 s**-1"
        }
    },
    {
        "variable": "vo",
        "name": "vorticity",
        "desc": "Vorticity (relative)",
        "units": "s**-1",
        "convert": {
            "constant": 100000,
            "operation": "multiply",
            "units": "10**-5 s**-1"
        }
    },
]


class ECMWFOpenData(DataIngest):
    def __init__(self, dataset_id, output_dir, cleanup_old_data=True, vector_db_conn_conn_params=None):
        super().__init__(dataset_id=dataset_id, output_dir=output_dir, cleanup_old_data=cleanup_old_data)

        self.surface_level_params = SURFACE_LEVEL_PARAMS
        self.pressure_level_params = PRESSURE_LEVELS_PARAMS
        self.pressure_levels = PRESSURE_LEVELS

        self.vector_db_conn_conn_params = vector_db_conn_conn_params

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
            "param": [p.get("variable") for p in self.surface_level_params],
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
            self.surface_level_params,
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
                        data_dir = f"{self.output_dir}/{namespace}"

                        for time_index, t in enumerate(ds.time.values):
                            data_datetime = pd.to_datetime(str(t))
                            date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

                            param_p_filename = f"{data_dir}/{namespace}_{date_str}.tif"
                            # create directory if not exists
                            Path(param_p_filename).parent.absolute().mkdir(parents=True, exist_ok=True)
                            # select data for time and pressure level
                            data_array = ds[data_var].isel(time=time_index, plev=p_index)

                            if p.get("convert"):
                                convert_config = p.get("convert")
                                data_array = self.convert_units(data_array, convert_config)

                            # save data as geotiff
                            data_array.rio.to_raster(param_p_filename, driver="COG")

                            vectors_config = p.get("vectors")
                            if vectors_config:
                                self.handle_vector_creation(vectors_config, param_p_filename, namespace, date_str)

                        # cleanup old forecasts before ingestion
                        self.cleanup_old_data(latest_str, data_dir)
                        # send ingest command
                        ingest_payload = {
                            "namespace": f"-n {namespace}",
                            "path": f"-p {data_dir}",
                            "datatype": "-t tif",
                            "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                        }

                        logging.info(
                            f"[ECMWF_FORECAST]: Sending ingest command for namespace: {namespace}")

                        self.send_ingest_command(ingest_payload)
                else:
                    namespace = f"{file_prefix}"
                    data_dir = f"{self.output_dir}/{namespace}"

                    for i, t in enumerate(ds.time.values):
                        data_datetime = pd.to_datetime(str(t))
                        date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

                        param_t_filename = f"{data_dir}/{namespace}_{date_str}.tif"
                        # create directory if not exists
                        Path(param_t_filename).parent.absolute().mkdir(parents=True, exist_ok=True)

                        data_array = ds[data_var].isel(time=i)

                        if p.get("convert"):
                            convert_config = p.get("convert")
                            data_array = self.convert_units(data_array, convert_config)

                        # nodata_value = data_array.encoding.get('nodata', data_array.encoding.get('_FillValue'))
                        #
                        # # check that nodata is not nan
                        # if np.isnan(nodata_value):
                        #     data_array = data_array.rio.write_nodata(-9999, encoded=True)

                        # save data as geotiff
                        data_array.rio.to_raster(param_t_filename, driver="COG")

                        vectors_config = p.get("vectors")
                        if vectors_config:
                            self.handle_vector_creation(vectors_config, param_t_filename, namespace, date_str)

                    # cleanup old forecasts before ingestion
                    self.cleanup_old_data(latest_str, data_dir)
                    # send ingest command
                    ingest_payload = {
                        "namespace": f"-n {namespace}",
                        "path": f"-p {data_dir}",
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

    def handle_vector_creation(self, vectors_config, param_t_filename, namespace, date_str, ):
        for vector_config in vectors_config:
            vector_type = vector_config.get("type")
            vector_options = vector_config.get("options")

            # handle contours
            if vector_type == "contour":
                attr_name = vector_options.get("attr_name")
                interval = vector_options.get("interval")

                logging.info(f"Generating contours for namespace: {namespace}")

                self.create_contour_data(param_t_filename, self.vector_db_conn_conn_params,
                                         date_str, namespace, attr_name, interval)
