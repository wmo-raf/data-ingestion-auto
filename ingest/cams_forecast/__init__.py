import datetime
import logging
import os
import tempfile
from pathlib import Path

from ingest import DataIngest
import cdsapi
import xarray as xr
import pandas as pd

CONFIG = {
    "params": [
        {
            "variable": "particulate_matter_2.5um",
            "data_var": "pm2p5",
            "name": "cams_forecast_pm2p5",
            "desc": "Particulate Matter 2.5"
        }
    ],
    'lead_times': [hour for hour in range(0, 120 + 1)]
}


class CamsForecast(DataIngest):
    def __init__(self, dataset_id, output_dir, api_key, cleanup_old_data=True):
        super().__init__(dataset_id=dataset_id, output_dir=output_dir, cleanup_old_data=cleanup_old_data)
        self.params = CONFIG.get("params")
        self.lead_times = CONFIG.get("lead_times")

        self.cams_url = "https://ads.atmosphere.copernicus.eu/api/v2"
        self.client = cdsapi.Client(url=self.cams_url, key=api_key, quiet=True, error_callback=self.error_callback)

    @staticmethod
    def error_callback(*args):
        if args[0].startswith("Reason"):
            logging.warning(f"[CAMS_FORECAST]: CDS API message: {args[1]}")

    def run(self, **kwargs):
        logging.info("[CAMS_FORECAST]: Trying...")
        request = {
            "dataset": "cams-global-atmospheric-composition-forecasts",
            "options": {
                'variable': [var["variable"] for var in self.params],
                'time': '00:00',
                'leadtime_hour': self.lead_times,
                'type': 'forecast',
                'format': 'netcdf',
            }
        }

        state = self.get_state() or {}
        last_update = state.get("last_update")

        if last_update:
            next_date = datetime.datetime.fromisoformat(last_update) + datetime.timedelta(hours=24)
        else:
            current_datetime = datetime.datetime.now()
            current_date = current_datetime.date()
            midnight = datetime.time(0, 0, 0)
            next_date = datetime.datetime.combine(current_date, midnight)

        data_download_file = tempfile.NamedTemporaryFile(delete=False, suffix=".nc").name

        options = request.get("options")

        # update date
        options.update({"date": next_date.strftime("%Y-%m-%d")})

        try:
            logging.info(f"[CAMS_FORECAST]: Trying download for date: {next_date}")
            self.client.retrieve(request.get("dataset"), options, data_download_file)
        except Exception:
            logging.info(f"[CAMS_FORECAST]: Data not downloaded. Skipping...")
            return

        ds = xr.open_dataset(data_download_file, engine="rasterio")
        ds.rio.write_crs("epsg:4326", inplace=True)

        for param in self.params:
            data_var = param.get("data_var")
            namespace = param.get("name")
            data_dir = f"{self.output_dir}/{namespace}"

            if data_var in ds.variables:
                for t_index, dt in enumerate(ds.time.values):
                    data_datetime = pd.to_datetime(str(dt))
                    date_str = data_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    # output filename
                    param_t_filename = f"{data_dir}/{namespace}_{date_str}.tif"

                    # create output directory if it does not exist
                    Path(param_t_filename).parent.absolute().mkdir(parents=True, exist_ok=True)

                    data_array = ds[data_var].isel(time=t_index)
                    # data_array.attrs['_FillValue'] = -9999.0
                    # data_array = data_array.rio.write_nodata(-9999, encoded=True)
                    units = data_array.attrs.get('units')
                    if units and isinstance(units, tuple):
                        data_array.attrs['units'] = units[0]

                    logging.info(f"[CAMS_FORECAST]: Saving {namespace} data for date: {date_str}")
                    data_array.rio.to_raster(param_t_filename, driver="COG", compress="DEFLATE")

            ingest_payload = {
                "namespace": f"-n {namespace}",
                "path": f"-p {data_dir}",
                "datatype": "-t tif",
                "args": "-x -conf /rulesets/namespace_yyy-mm-ddTH.tif.json"
            }

            logging.info(
                f"[CAMS_FORECAST]: Sending ingest command for {namespace} and starting date: {next_date.isoformat()}")
            self.send_ingest_command(ingest_payload)

        # cleanup
        os.remove(data_download_file)

        # update state
        self.update_state({"last_update": next_date.isoformat()})
