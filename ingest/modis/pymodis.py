import datetime
import math
import re

import numpy as np
import requests
from pyproj import Proj
from html.parser import HTMLParser

from ingest.auth import BearerAuth
from ingest.utils import download_to_file


def get_modland_grids(min_lon, max_lon, min_lat, max_lat):
    # Define function to convert lon/lat to sinusoidal x/y coordinates
    def lon_lat_to_xy(lon, lat):
        r = 6371007.181
        x = r * np.radians(lon) * np.cos(np.radians(lat))
        y = r * np.radians(lat)
        return x, y

    # Convert bbox to sinusoidal x/y coordinates
    min_x, min_y = lon_lat_to_xy(min_lon, min_lat)
    max_x, max_y = lon_lat_to_xy(max_lon, max_lat)

    # Determine range of grid indices that intersect with bbox
    ntile_vert = 18
    ntile_horiz = 36
    iv_min = math.floor((90.0 + min_y) / 10.0)
    iv_max = math.floor((90.0 + max_y) / 10.0)
    ih_min = math.floor((180.0 + min_x) / 10.0)
    ih_max = math.floor((180.0 + max_x) / 10.0)

    # Loop through grid cells that intersect with bbox and add to list
    grids = []
    for iv in range(iv_min, iv_max + 1):
        for ih in range(ih_min, ih_max + 1):
            grids.append(f'h{iv:02d}v{ih:02d}')

    return grids


class ModisHtmlParser(HTMLParser):
    """A class to parse HTML
       :param fh: content of http request
    """

    def __init__(self, fh):
        """Function to initialize the object"""
        HTMLParser.__init__(self)
        self.file_ids = []
        self.feed(str(fh))

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            attr_d = dict(attrs)
            self.file_ids.append(attr_d['href'].replace('/', ''))

    def get_all(self):
        """Return everything"""
        return self.file_ids

    def get_dates(self):
        """Return a list of directories with date"""
        regex = re.compile('(\d{4})[/.-](\d{2})[/.-](\d{2})$')
        alldata = set([elem for elem in self.file_ids if regex.match(elem)])
        return sorted(list(alldata))

    def get_tiles(self, product_code, tiles=None, jpeg=False):
        """Return a list of files to download
           :param str product_code: the code of MODIS product that we are going to
                            analyze
           :param list tiles: the list of tiles to consider
           :param bool jpeg: True to also check for jpeg data
        """
        final_list = []
        for i in self.file_ids:
            # distinguish jpg from hdf by where the tileID is within the string
            # jpgs have the tileID at index 3, hdf have tileID at index 2
            name = i.split('.')
            # if product is not in the filename, move to next filename in list

            if not name.count(product_code):
                continue

            # skip xml
            if name.count("xml"):
                continue

            # if tiles are not specified and the file is not a jpg, add to list
            if not tiles and not (name.count('jpg') or name.count('BROWSE')):
                final_list.append(i)
            # if tiles are specified
            if tiles:
                for tile in tiles:
                    # if a tileID is at index 3 and jpgs are to be downloaded
                    if jpeg and tile == name[3]:
                        final_list.append(i)
                    # if a tileID is at in index 2, it is known to be HDF
                    elif tile == name[2]:
                        final_list.append(i)
        return final_list


def urljoin(*args):
    """Joins given arguments into a url. Trailing but not leading slashes are
    stripped for each argument.
    http://stackoverflow.com/a/11326230
    :return: a string
    """

    return "/".join([str(x).rstrip('/') for x in args])


class ModisDownloader(object):
    def __init__(self, auth_token, data_extent, product_code, base_url="https://e4ftl01.cr.usgs.gov",
                 product_path="MOLA",
                 product_version=None):
        self.base_url = base_url
        self.auth = BearerAuth(auth_token)
        self.product_version = product_version

        self.product_path = product_path
        self.product_code = product_code
        self.data_extent = data_extent

        self.product_code_with_version = self.product_code

        if self.product_version:
            self.product_code_with_version = f"{self.product_code}.{self.product_version}"

        self.data_path = urljoin(self.product_path, self.product_code_with_version)

    def check_availability_for_date(self, date):
        date = datetime.datetime.fromisoformat(date)
        data_date = date.strftime("%Y.%m.%d")
        url = urljoin(self.base_url, self.data_path, data_date)
        r = requests.get(url)

        if r.status_code == 404:
            return False, url

        r.raise_for_status()

        return True, url

    @property
    def extent_tiles(self):
        tiles = get_modland_grids(*self.data_extent)

        return tiles

    def get_date_tile_files(self, date_url, tiles=None):
        r = requests.get(date_url)
        html = ModisHtmlParser(r.text)
        tiles = html.get_tiles(self.product_code, tiles=tiles)
        return tiles

    def download_hdf_file(self, date_url, hdf_tile_path, out_file):
        file_path_url = urljoin(date_url, hdf_tile_path)
        return download_to_file(file_path_url, out_file, auth=self.auth)
