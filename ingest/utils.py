import json
import logging
import os
import re
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

import pytz
import requests
import rioxarray as rxr
from dateutil import parser

from ingest.config import SETTINGS
from ingest.errors import UnknownDataConvertOperation

DATASET_STATE_DIR = SETTINGS.get("DATASET_STATE_DIR")

DATASET_STATE_FILE = os.path.join(DATASET_STATE_DIR, "state.json")


def copy_with_metadata(source, target):
    """Copy file with all its permissions and metadata.

    Lifted from https://stackoverflow.com/a/43761127/2860309
    :param source: source file name
    :param target: target file name
    """
    # copy content, stat-info (mode too), timestamps...
    shutil.copy2(source, target)
    # copy owner and group
    st = os.stat(source)
    os.chown(target, st[stat.ST_UID], st[stat.ST_GID])


def atomic_write(file_contents, target_file_path, mode="w"):
    """Write to a temporary file and rename it to avoid file corruption.
    Attribution: @therightstuff, @deichrenner, @hrudham
    :param file_contents: contents to be written to file
    :param target_file_path: the file to be created or replaced
    :param mode: the file mode defaults to "w", only "w" and "a" are supported
    """
    # Use the same directory as the destination file so that moving it across
    # file systems does not pose a problem.
    temp_file = tempfile.NamedTemporaryFile(
        delete=False,
        dir=os.path.dirname(target_file_path))
    try:
        # preserve file metadata if it already exists
        if os.path.exists(target_file_path):
            copy_with_metadata(target_file_path, temp_file.name)
        with open(temp_file.name, mode) as f:
            f.write(file_contents)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_file.name, target_file_path)
    finally:
        if os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except:
                pass


def convert_nc_to_geotiff(in_file_path, time_index, out_file_path):
    rds = rxr.open_rasterio(in_file_path)

    try:
        rds.isel(time=time_index).rio.to_raster(out_file_path, driver="COG")
    except Exception as e:
        raise e
    finally:
        rds.close()

    return True


def write_empty_state(dataset_id):
    content = {}
    if dataset_id:
        content[dataset_id] = {"last_update": ""}

    atomic_write(json.dumps(content, indent=4), DATASET_STATE_FILE)

    if dataset_id:
        return content[dataset_id]

    return content


def read_state(dataset_id):
    # create state file if it does not exist
    if not os.path.isfile(DATASET_STATE_FILE):
        with open(DATASET_STATE_FILE, mode='w') as f:
            f.write("{}")
    try:
        logging.debug(f"[STATE]: Opening state file {DATASET_STATE_FILE}")
        with open(DATASET_STATE_FILE, 'r') as f:
            state = json.load(f)
    except json.decoder.JSONDecodeError:
        state = write_empty_state(dataset_id)

    if state.get(dataset_id):
        return state.get(dataset_id)

    return None


def update_state(dataset_id, new_state):
    with open(DATASET_STATE_FILE, 'r') as f:
        state = json.load(f)

    state[dataset_id] = new_state

    atomic_write(json.dumps(state, indent=4), DATASET_STATE_FILE)


def download_file_temp(url, auth=None, timeout=None, suffix=None):
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    with requests.get(url, stream=True, auth=auth, timeout=timeout) as r:
        r.raise_for_status()
        tmp_file.write(r.content)
    return tmp_file.name


def delete_past_data_files(latest_date_str, file_dir):
    latest_date = parser.parse(latest_date_str).astimezone(pytz.utc)
    pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)'

    count = 0

    for root, directories, files in os.walk(file_dir):
        for filename in files:
            file_path = os.path.join(root, filename)

            match = re.search(pattern, file_path)

            if match:
                # If a match is found, extract the date and time
                datetime_string = match.group(1)

                file_date = parser.parse(datetime_string)

                if file_date < latest_date:
                    logging.debug(f"[CLEANUP]: Deleting file {file_path}")
                    os.remove(file_path)
                    count += 1

    logging.info(f"[CLEANUP]: Deleted: {count} files from directory: {file_dir}")


def convert_data(data_array, constant, operation):
    if operation == "multiply":
        return data_array * constant
    if operation == "divide":
        return data_array / constant
    if operation == "subtract":
        return data_array - constant
    if operation == "add":
        return data_array + constant

    raise UnknownDataConvertOperation(f"Unknown operation: {operation}")


def generate_contour_geojson(data_file, out_dir, options):
    data_file_name = f"{Path(data_file).stem}.geojson"

    geojson_out = os.path.join(out_dir, data_file_name)

    attr_name = options.get("attr_name")
    interval = options.get("interval")

    # gdal_contour command
    command = f"gdal_contour -a {attr_name} {data_file} {geojson_out} -i {interval}"

    # Execute the command using subprocess
    subprocess.run(command, shell=True, check=True)

    return geojson_out


def create_contour_data(raster_data_file, attr_name, interval, out_dir):
    # handle contour generation
    contour_options = {
        "attr_name": attr_name,
        "interval": interval
    }

    geojson_out_file = generate_contour_geojson(raster_data_file, out_dir, contour_options)

    return geojson_out_file
