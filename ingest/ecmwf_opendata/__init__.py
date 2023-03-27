import logging
import os
import tempfile
from pathlib import Path

import numpy as np
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
    {
        "variable": "10u",
        "name": "u_wind",
        "desc": "10 metre U wind component",
        "units": "m s**-1",
    },
    {"variable": "10v", "name": "v_wind", "desc": "10 metre V wind component", "units": "m s**-1"},
    {
        "derived": True,
        "variable": "10u",
        "name": "wind_speed",
        "desc": "Wind Speed",
        "units": "m s**-1",
        "derived_config": {
            "type": "wind_speed",
            "u_var": "10u",
            "v_var": "10v"
        }
    }
]

PRESSURE_LEVELS = [1000, 925, 850, 700, 500, 300, 250, 200, 50]
# PRESSURE_LEVELS = [1000]

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
            "constant": 1000,
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
        "derived": True,
        "variable": "u",
        "name": "wind_speed",
        "desc": "Wind Speed",
        "units": "m s**-1",
        "derived_config": {
            "type": "wind_speed",
            "u_var": "u",
            "v_var": "v"
        }
    },
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

    def run(self):

        # get surface data
        latest_str = self.retrieve_surface_data()

        # get pressure levels data
        latest_str = self.retrieve_pressure_levels_data()

        if latest_str:
            # update state
            self.update_state({"last_update": latest_str})

    def retrieve_surface_data(self):
        logging.info('[ECMWF_FORECAST]: Trying Surface Data...')
        request = {
            "stream": "oper",
            "type": "fc",
            "levtype": "sfc",
            "param": [p.get("variable") for p in self.surface_level_params if not p.get("derived")],
            "time": 0,
            "step": self.steps
        }

        logging.info('[ECMWF_FORECAST]: Checking remote Surface Data latest date...')
        latest = self.get_latest_date(request)

        if not latest:
            return None

        dataset_state = self.get_state()

        logging.info(f'[ECMWF_FORECAST]: Latest remote Surface Data date is: {latest}')
        latest_str = latest.isoformat()

        if dataset_state and dataset_state.get("last_update") and dataset_state.get("last_update") == latest_str:
            logging.info(f'[ECMWF_FORECAST]: No Surface Data Update required. Skipping...')
            return

        temp_file = tempfile.NamedTemporaryFile(delete=False)

        # update target file name
        request.update({"target": temp_file.name})

        logging.info(f"[ECMWF_FORECAST]: Downloading Surface Data forecast for date: {latest}...")
        self.client.retrieve(request)

        file_prefix = f"{request.get('stream')}_{request.get('type')}"
        level_type = f"{request.get('levtype')}"

        self.process_surface_levels_data(
            temp_file.name,
            file_prefix,
            level_type,
            latest_str
        )

        return latest_str

    def retrieve_pressure_levels_data(self):
        logging.info('[ECMWF_FORECAST]: Trying Pressure Levels Data...')
        request = {
            "stream": "oper",
            "type": "fc",
            "levtype": "pl",
            "levelist": self.pressure_levels,
            "param": [p.get("variable") for p in self.pressure_level_params if not p.get("derived")],
            "time": 0,
            "step": self.steps
        }

        logging.info('[ECMWF_FORECAST]: Checking remote Pressure Levels Data latest date...')
        latest = self.get_latest_date(request)
        dataset_state = self.get_state()

        if not latest:
            return None

        logging.info(f'[ECMWF_FORECAST]: Latest remote Pressure Levels Data date is: {latest}')
        latest_str = latest.isoformat()

        if dataset_state and dataset_state.get("last_update") and dataset_state.get("last_update") == latest_str:
            logging.info(f'[ECMWF_FORECAST]: No Pressure Levels Data Update required. Skipping...')
            return

        temp_file = tempfile.NamedTemporaryFile(delete=False)

        # update target file name
        request.update({"target": temp_file.name})

        logging.info(f"[ECMWF_FORECAST]: Downloading Pressure Levels forecast data for date: {latest}...")
        self.client.retrieve(request)

        file_prefix = f"{request.get('stream')}_{request.get('type')}"
        level_type = f"{request.get('levtype')}"

        self.process_pressure_levels_data(
            temp_file.name,
            file_prefix,
            level_type,
            latest_str
        )

        return latest_str

    def process_surface_levels_data(self, grib_file, file_prefix, level_type, latest_str):
        logging.info(f'[ECMWF_FORECAST]: Processing Surface Data for date: {latest_str}...')

        # convert grib to netcdf
        nc_file = self.grib_to_nc(grib_file)

        # open nc and write projection info
        ds = xr.open_dataset(nc_file)
        ds.rio.write_crs("epsg:4326", inplace=True)

        # process each surface level param
        for param in self.surface_level_params:
            data_var = param.get("variable")
            param_name = param.get("name")
            derived = param.get("derived")

            if data_var in ds.variables:
                namespace = f"{file_prefix}_{param_name}_{level_type}"
                data_dir = f"{self.output_dir}/{namespace}"

                # process each timestamp
                for i, t in enumerate(ds.time.values):
                    data_datetime = pd.to_datetime(str(t))
                    date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    # output filename
                    param_t_filename = f"{data_dir}/{namespace}_{date_str}.tif"
                    # create output directory if it does not exist
                    Path(param_t_filename).parent.absolute().mkdir(parents=True, exist_ok=True)

                    if not derived:
                        logging.info(f'[ECMWF_FORECAST]: Processing Surface Data param: {param_name} and time {t}')
                        # get variable data for time
                        data_array = ds[data_var].isel(time=i)

                        convert_config = param.get("convert")
                        if convert_config:
                            data_array = self.convert_units(data_array, convert_config)

                        # save data as geotiff
                        data_array.rio.to_raster(param_t_filename, driver="COG")

                        # generate vector data from param e.g contours
                        vectors_config = param.get("vectors")
                        if vectors_config:
                            self.handle_vector_generation(vectors_config, param_t_filename, namespace, date_str,
                                                          latest_str)
                    else:
                        logging.info(
                            f'[ECMWF_FORECAST]: Processing Derived Surface Data param: {param_name} and time {t}')
                        derived_config = param.get("derived_config")
                        derived_type = derived_config.get("type")

                        # generate wind speed
                        if derived_type == "wind_speed":
                            u_var = derived_config.get("u_var")
                            v_var = derived_config.get("v_var")
                            units = derived_config.get("units")

                            if u_var in ds.variables and v_var in ds.variables:
                                u_data = ds[u_var].isel(time=i)
                                v_data = ds[v_var].isel(time=i)
                                wind_speed = self.calculate_wind_speed(u_data, v_data)

                                if units:
                                    wind_speed.attrs["units"] = units

                                logging.info(
                                    f'[ECMWF_FORECAST]: Saving Surface Data Derived {param_t_filename}')

                                wind_speed.rio.to_raster(param_t_filename, driver="COG")

                # cleanup old forecasts before ingestion
                self.cleanup_old_data(latest_str, data_dir)

                # prepare ingestion payload
                ingest_payload = {
                    "namespace": f"-n {namespace}",
                    "path": f"-p {data_dir}",
                    "datatype": "-t tif",
                    "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                }
                logging.info(f"[ECMWF_FORECAST]: Sending ingest command for namespace: {namespace}")

                # send ingest command
                self.send_ingest_command(ingest_payload)

        # delete nc file
        os.remove(nc_file)

    def process_pressure_levels_data(self, grib_file, file_prefix, level_type, latest_str):
        logging.info(f'[ECMWF_FORECAST]: Processing Pressure Levels Data for date: {latest_str}...')
        # convert grib to netcdf
        nc_file = self.grib_to_nc(grib_file)

        # open nc and write projection info
        ds = xr.open_dataset(nc_file)
        ds.rio.write_crs("epsg:4326", inplace=True)

        # process each pressure level param
        for param in self.pressure_level_params:
            data_var = param.get("variable")
            param_name = param.get("name")
            derived = param.get("derived")

            if data_var in ds.variables:
                # process each pressure level
                for p_index, p_lev in enumerate(ds.plev.values):
                    # convert to hPa
                    p_hpa = int(p_lev / 100)

                    namespace = f"{file_prefix}_{param_name}_{level_type}_{p_hpa}"
                    data_dir = f"{self.output_dir}/{namespace}"

                    for time_index, t in enumerate(ds.time.values):
                        data_datetime = pd.to_datetime(str(t))
                        date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

                        param_p_filename = f"{data_dir}/{namespace}_{date_str}.tif"
                        # create directory if not exists
                        Path(param_p_filename).parent.absolute().mkdir(parents=True, exist_ok=True)

                        if not derived:
                            logging.info(
                                f'[ECMWF_FORECAST]: Processing Pressure Levels Data for param param: {param_name}, time {t} PLevel: {p_hpa} hPa')
                            # select data for time and pressure level
                            data_array = ds[data_var].isel(time=time_index, plev=p_index)

                            convert_config = param.get("convert")
                            if convert_config:
                                data_array = self.convert_units(data_array, convert_config)
                            # save data as geotiff
                            data_array.rio.to_raster(param_p_filename, driver="COG")

                            vectors_config = param.get("vectors")
                            if vectors_config:
                                self.handle_vector_generation(vectors_config, param_p_filename, namespace, date_str,
                                                              latest_str)
                        else:
                            logging.info(
                                f'[ECMWF_FORECAST]: Processing Derived Pressure Level Data for param: {param_name}, time {t}, PLevel: {p_hpa} hPa')
                            derived_config = param.get("derived_config")
                            derived_type = derived_config.get("type")

                            # generate wind speed
                            if derived_type == "wind_speed":
                                u_var = derived_config.get("u_var")
                                v_var = derived_config.get("v_var")
                                units = derived_config.get("units")

                                if u_var in ds.variables and v_var in ds.variables:
                                    u_data = ds[u_var].isel(time=time_index, plev=p_index)
                                    v_data = ds[v_var].isel(time=time_index, plev=p_index)
                                    wind_speed = self.calculate_wind_speed(u_data, v_data)

                                    if units:
                                        wind_speed.attrs["units"] = units

                                    logging.info(
                                        f'[ECMWF_FORECAST]: Saving Pressure Level Derived: {param_p_filename}')
                                    wind_speed.rio.to_raster(param_p_filename, driver="COG")

                    # cleanup old forecasts before ingestion
                    self.cleanup_old_data(latest_str, data_dir)
                    # prepare ingest payload
                    ingest_payload = {
                        "namespace": f"-n {namespace}",
                        "path": f"-p {data_dir}",
                        "datatype": "-t tif",
                        "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
                    }

                    logging.info(
                        f"[ECMWF_FORECAST]: Sending ingest command for namespace: {namespace}")
                    # send ingest command
                    self.send_ingest_command(ingest_payload)

        # delete nc file
        os.remove(nc_file)

    def grib_to_nc(self, grib_file_path):
        # convert to nc
        nc_out_tmp = tempfile.NamedTemporaryFile(delete=False)

        logging.info(f"[ECMWF_FORECAST]: Converting grib to nc ...")
        self.grib_to_netcdf(grib_file_path, nc_out_tmp.name)

        # delete grib file
        os.remove(grib_file_path)

        return nc_out_tmp.name

    def handle_vector_generation(self, vectors_config, param_t_filename, namespace, date_str, latest_date_str):
        for vector_config in vectors_config:
            vector_type = vector_config.get("type")
            vector_options = vector_config.get("options")

            # handle contours
            if vector_type == "contour":
                attr_name = vector_options.get("attr_name")
                interval = vector_options.get("interval")

                logging.info(f"Generating contours for namespace: {namespace}")

                self.create_contour_data(param_t_filename, self.vector_db_conn_conn_params,
                                         date_str, namespace, attr_name, interval, latest_date_str=latest_date_str)

    @staticmethod
    def calculate_wind_speed(u_data, v_data):
        return np.sqrt(u_data ** 2 + v_data ** 2)
