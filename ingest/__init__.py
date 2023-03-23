import codecs
import hashlib
import hmac
import logging
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import fiona
import requests
from shapely.geometry import shape

from ingest.config import SETTINGS
from ingest.errors import ParameterMissing
from ingest.raster_vector import VectorDbManager
from ingest.utils import read_state, update_state, delete_past_data_files, convert_data, generate_contour_geojson, \
    create_contour_data

GSKY_INGEST_LAYER_WEBHOOK_URL = SETTINGS.get("GSKY_INGEST_LAYER_WEBHOOK_URL")
GSKY_WEBHOOK_SECRET = SETTINGS.get("GSKY_WEBHOOK_SECRET")


class DataIngest(object):
    def __init__(self, dataset_id, output_dir, task_timeout=10 * 60, cleanup_old_data=True):
        if not dataset_id:
            raise ParameterMissing("dataset_id not provided")

        if not output_dir:
            raise ParameterMissing("output_dir not provided")

        self.dataset_id = dataset_id
        self.output_dir = output_dir
        self.africa_shp_path = SETTINGS.get("AFRICA_SHP_PATH")
        self.cleanup_data = cleanup_old_data
        self.task_timeout = task_timeout

    def run(self, **kwargs):
        raise NotImplementedError

    def task(self):
        logging.info(f"Scheduling {self.__class__.__name__}...")

        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.run)

            try:
                future.result(timeout=self.task_timeout)
            except TimeoutError:
                logging.warning(f"{self.__class__.__name__} took too long to finish. Cancelling the task...")
                future.cancel()

    def get_state(self):
        logging.debug('Reading state')
        return read_state(self.dataset_id)

    def update_state(self, new_state):
        logging.debug('Writing state')
        # get existing state
        state = self.get_state() or {}
        state.update({**new_state})
        update_state(self.dataset_id, state)

    def clip_to_africa(self, ds):
        # read shapefile
        shp = fiona.open(self.africa_shp_path)
        # convert first feature to shapely shape
        geom = shape(shp[0]['geometry'])

        ds = ds.rio.clip([geom], 'epsg:4326', drop=True)

        return ds

    @staticmethod
    def convert_units(data_array, convert_config):
        operation = convert_config.get("operation")
        constant = convert_config.get("constant")
        units = convert_config.get("units")

        if operation and constant:
            logging.info(f"Performing operation : {operation} on data")
            data_array = convert_data(data_array, constant, operation)

            if units:
                data_array.attrs["units"] = units
        return data_array

    @staticmethod
    def grib_to_netcdf(input_file, output_file):
        """
        Converts a GRIB file to netCDF format using CDO.

        Args:
            input_file (str): Path to the input GRIB file.
            output_file (str): Path to the output netCDF file.

        Returns:
            output_file (str): Path to the output netCDF file.
        """
        # Construct the CDO command to convert the file
        command = f"cdo -f nc copy {input_file} {output_file}"

        # Execute the command using subprocess
        subprocess.run(command, shell=True, check=True)

        return output_file

    @staticmethod
    def send_ingest_command(payload):
        if GSKY_INGEST_LAYER_WEBHOOK_URL and GSKY_WEBHOOK_SECRET:
            request = requests.Request(method="POST", url=f"{GSKY_INGEST_LAYER_WEBHOOK_URL}", data=payload, headers={})
            prepped = request.prepare()
            # generate signature for auth
            signature = hmac.new(codecs.encode(GSKY_WEBHOOK_SECRET), codecs.encode(prepped.body),
                                 digestmod=hashlib.sha256)
            prepped.headers['X-Gsky-Signature'] = signature.hexdigest()

            with requests.Session() as session:
                response = session.send(prepped)
                logging.info(f"[INGEST]: Ingest command sent successfully for namespace {payload.get('namespace')}")
                logging.info(response.text)
            return True

        return False

    def cleanup_old_data(self, latest_date_str, data_dir):
        if self.cleanup_data:
            logging.info(f"[DATASET CLEANUP]: Cleaning up old {self.dataset_id} files for date: {latest_date_str}")
            delete_past_data_files(latest_date_str, data_dir)

    @staticmethod
    def create_contour_data(raster_file_path, conn_params, date_str, table_name, attr_name, interval,
                            latest_date_str=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            geojson_file = create_contour_data(raster_file_path,
                                               attr_name=attr_name,
                                               interval=interval,
                                               out_dir=temp_dir)
            data_columns = [attr_name]

            db = VectorDbManager(
                conn_params=conn_params,
                schema_name="pgadapter",
                table_name=table_name,
                geom_type="LineString",
                data_columns=data_columns,
                srid=4326
            )

            # insert data
            db.insert_update_data(date_str, geojson_file, latest_date_str=latest_date_str)
