import logging
import os.path
import tempfile

from ingest import DataIngest
# from ingest.modis.convertmodis import CreateMosaicGDAL
from ingest.modis.pymodis import ModisDownloader
import rioxarray as rxr

MODIS_DATA_PRODUCTS = [
    {
        "title": "Vegetation Indices 16-Day L3 Global 250m",
        "instrument": "MODIS",
        "satellite": "aqua",
        "product_path": "MOLA",
        "product_code": "MYD13Q1",
        "product_version": "061",
        "sub_datasets": [
            {
                "title": "Modis 250m 16 Days NDVI",
                "hdf_sub_dataset_name": "250m 16 days NDVI",
                "name": "modis_250m_16_days_ndvi",
                "convert": {
                    "operation": "divide",
                    "constant": 10000,
                },
            }
        ]

    }
]


class ModisData(DataIngest):
    def __init__(self, dataset_id, output_dir, data_extent, auth_token):
        super().__init__(dataset_id, output_dir)
        self.data_extent = data_extent
        self.auth_token = auth_token
        self.products = MODIS_DATA_PRODUCTS

    def run(self, **kwargs):
        for product in self.products:
            product_path = product.get("product_path")
            product_code = product.get("product_code")
            product_version = product.get("product_version", None)
            satellite = product.get("satellite")

            downloader = ModisDownloader(auth_token=self.auth_token, product_path=product_path,
                                         product_code=product_code, data_extent=self.data_extent,
                                         product_version=product_version)

            next_date_str = "2023-02-26T00:00:00"
            available, date_url = downloader.check_availability_for_date(next_date_str)
            if not available:
                logging.info(f"[MODIS_DATA]: Data not available for date: {next_date_str}. Skipping...")

            if available and date_url:
                extent_tiles = downloader.extent_tiles

                date_available_tiles = downloader.get_date_tile_files(date_url, tiles=extent_tiles)

                # with tempfile.TemporaryDirectory() as temp_dir:

                data_dir = os.path.join(self.output_dir, "input")
                files = []
                for hdf_tile_path in date_available_tiles:
                    out_file = os.path.join(data_dir, hdf_tile_path)
                    files.append(out_file)
                    if not os.path.exists(out_file):
                        logging.info(f"[MODIS_DATA]: Downloading data for tile: {hdf_tile_path}")
                        downloader.download_hdf_file(date_url, hdf_tile_path, out_file)

                logging.info(f"[MODIS_DATA]: Finished Downloading data for all the tiles: {len(date_available_tiles)}")

                # for p in self.products:
                #     product_name = product.get("name")
                #     product_subset_name = product.get("subset_name")
                #     mosaic_out = os.path.join(data_dir, f"{product_name}-mosaic.tif")
                # mosaic = CreateMosaicGDAL(files, [product_subset_name])
                # mosaic.run(mosaic_out)
                #
                # data_array = rxr.open_rasterio(mosaic_out)
                #
                # convert_config = product.get("convert")
                # if convert_config:
                #     data_array = self.convert_units(data_array, convert_config)
