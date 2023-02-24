import codecs
import hashlib
import hmac
import logging
import subprocess

import fiona
import requests
from shapely.geometry import shape

from ingest.config import SETTINGS
from ingest.errors import ParameterMissing
from ingest.utils import read_state, update_state

GSKY_INGEST_LAYER_WEBHOOK_URL = SETTINGS.get("GSKY_INGEST_LAYER_WEBHOOK_URL")
GSKY_WEBHOOK_SECRET = SETTINGS.get("GSKY_WEBHOOK_SECRET")


class DataIngest(object):
    def __init__(self, dataset_id, output_dir):
        if not dataset_id:
            raise ParameterMissing("dataset_id not provided")

        if not output_dir:
            raise ParameterMissing("output_dir not provided")

        self.dataset_id = dataset_id
        self.output_dir = output_dir
        self.africa_shp_path = SETTINGS.get("AFRICA_SHP_PATH")

    def run(self):
        raise NotImplementedError

    def get_state(self):
        logging.debug('Reading state')
        return read_state(self.dataset_id)

    def update_state(self, last_update):
        logging.debug('Writing state')
        new_state = {"last_update": last_update}
        update_state(self.dataset_id, new_state)

    def clip_to_africa(self, ds):
        # read shapefile
        shp = fiona.open(self.africa_shp_path)
        # convert first feature to shapely shape
        geom = shape(shp[0]['geometry'])

        ds = ds.rio.clip([geom], 'epsg:4326', drop=True)

        return ds

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
