import tempfile
from collections import OrderedDict

try:
    from slugify import slugify
except ImportError:
    raise ImportError('Python slugify library not found, please install '
                      'unicode-slugify for Python > 3 or slugify for Python < 3')

try:
    import osgeo.gdal as gdal
except ImportError:
    try:
        import gdal
    except ImportError:
        raise ImportError('Python GDAL library not found, please install '
                          'python-gdal')

try:
    import osgeo.osr as osr
except ImportError:
    try:
        import osr
    except ImportError:
        raise ImportError('Python GDAL library not found, please install '
                          'python-gdal')

RESAM_GDAL = ['AVERAGE', 'BILINEAR', 'CUBIC', 'CUBIC_SPLINE', 'LANCZOS',
              'MODE', 'NEAREST_NEIGHBOR']
SINU_WKT = 'PROJCS["Sinusoidal_Sanson_Flamsteed",GEOGCS["GCS_Unknown",' \
           'DATUM["D_unknown",SPHEROID["Unknown",6371007.181,"inf"]],' \
           'PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]' \
           ',PROJECTION["Sinusoidal"],PARAMETER["central_meridian",0],' \
           'PARAMETER["false_easting",0],PARAMETER["false_northing",0]' \
           ',UNIT["Meter",1]]'


def getResampling(res):
    """Return the GDAL resampling method

       :param str res: the string of resampling method
    """
    if res == 'AVERAGE':
        return gdal.GRA_Average
    elif res == 'BILINEAR' or res == 'BICUBIC':
        return gdal.GRA_Bilinear
    elif res == 'LANCZOS':
        return gdal.GRA_Lanczos
    elif res == 'MODE':
        return gdal.GRA_Mode
    elif res == 'NEAREST_NEIGHBOR':
        return gdal.GRA_NearestNeighbour
    elif res == 'CUBIC_CONVOLUTION' or res == 'CUBIC':
        return gdal.GRA_Cubic
    elif res == 'CUBIC_SPLINE':
        return gdal.GRA_CubicSpline


# =============================================================================
def raster_copy(s_fh, s_xoff, s_yoff, s_xsize, s_ysize, s_band_n,
                t_fh, t_xoff, t_yoff, t_xsize, t_ysize, t_band_n,
                nodata=None):
    """Copy a band of raster into the output file.

       Function copied from gdal_merge.py
    """
    if nodata is not None:
        return raster_copy_with_nodata(s_fh, s_xoff, s_yoff, s_xsize, s_ysize,
                                       s_band_n, t_fh, t_xoff, t_yoff, t_xsize,
                                       t_ysize, t_band_n, nodata)

    s_band = s_fh.GetRasterBand(s_band_n)
    t_band = t_fh.GetRasterBand(t_band_n)

    data = s_band.ReadRaster(s_xoff, s_yoff, s_xsize, s_ysize,
                             t_xsize, t_ysize, t_band.DataType)
    t_band.WriteRaster(t_xoff, t_yoff, t_xsize, t_ysize, data, t_xsize,
                       t_ysize, t_band.DataType)

    return 0


def raster_copy_with_nodata(s_fh, s_xoff, s_yoff, s_xsize, s_ysize, s_band_n,
                            t_fh, t_xoff, t_yoff, t_xsize, t_ysize, t_band_n,
                            nodata):
    """Copy a band of raster into the output file with nodata values.

       Function copied from gdal_merge.py
    """
    try:
        import numpy as Numeric
    except ImportError:
        import Numeric

    s_band = s_fh.GetRasterBand(s_band_n)
    t_band = t_fh.GetRasterBand(t_band_n)

    data_src = s_band.ReadAsArray(s_xoff, s_yoff, s_xsize, s_ysize,
                                  t_xsize, t_ysize)
    data_dst = t_band.ReadAsArray(t_xoff, t_yoff, t_xsize, t_ysize)

    nodata_test = Numeric.equal(data_src, nodata)
    to_write = Numeric.choose(nodata_test, (data_src, data_dst))

    t_band.WriteArray(to_write, t_xoff, t_yoff)

    return 0


class FileInfo:
    """A class holding information about a GDAL file.

       Class copied from gdal_merge.py

       :param str filename: Name of file to read.

       :return: 1 on success or 0 if the file can't be opened.
    """

    def init_from_name(self, filename):
        """Initialize file_info from filename"""
        fh = gdal.Open(filename)
        if fh is None:
            return 0

        self.filename = filename
        self.bands = fh.RasterCount
        self.xsize = fh.RasterXSize
        self.ysize = fh.RasterYSize
        self.band_type = fh.GetRasterBand(1).DataType
        self.block_size = fh.GetRasterBand(1).GetBlockSize()
        self.projection = fh.GetProjection()
        self.geotransform = fh.GetGeoTransform()
        self.ulx = self.geotransform[0]
        self.uly = self.geotransform[3]
        self.lrx = self.ulx + self.geotransform[1] * self.xsize
        self.lry = self.uly + self.geotransform[5] * self.ysize

        meta = fh.GetMetadata()
        if '_FillValue' in list(meta.keys()):
            self.fill_value = meta['_FillValue']
        elif fh.GetRasterBand(1).GetNoDataValue():
            self.fill_value = fh.GetRasterBand(1).GetNoDataValue()
        else:
            self.fill_value = None

        ct = fh.GetRasterBand(1).GetRasterColorTable()
        if ct is not None:
            self.ct = ct.Clone()
        else:
            self.ct = None

        return 1

    def copy_into(self, t_fh, s_band=1, t_band=1, nodata_arg=None):
        """Copy this files image into target file.

        This method will compute the overlap area of the file_info objects
        file, and the target gdal.Dataset object, and copy the image data
        for the common window area.  It is assumed that the files are in
        a compatible projection. no checking or warping is done.  However,
        if the destination file is a different resolution, or different
        image pixel type, the appropriate resampling and conversions will
        be done (using normal GDAL promotion/demotion rules).

        :param t_fh: gdal.Dataset object for the file into which some or all
                     of this file may be copied.
        :param s_band:
        :param t_band:
        :param nodata_arg:

        :return: 1 on success (or if nothing needs to be copied), and zero one
                 failure.

        """
        t_geotransform = t_fh.GetGeoTransform()
        t_ulx = t_geotransform[0]
        t_uly = t_geotransform[3]
        t_lrx = t_geotransform[0] + t_fh.RasterXSize * t_geotransform[1]
        t_lry = t_geotransform[3] + t_fh.RasterYSize * t_geotransform[5]

        # figure out intersection region
        tgw_ulx = max(t_ulx, self.ulx)
        tgw_lrx = min(t_lrx, self.lrx)
        if t_geotransform[5] < 0:
            tgw_uly = min(t_uly, self.uly)
            tgw_lry = max(t_lry, self.lry)
        else:
            tgw_uly = max(t_uly, self.uly)
            tgw_lry = min(t_lry, self.lry)

        # do they even intersect?
        if tgw_ulx >= tgw_lrx:
            return 1
        if t_geotransform[5] < 0 and tgw_uly <= tgw_lry:
            return 1
        if t_geotransform[5] > 0 and tgw_uly >= tgw_lry:
            return 1

        # compute target window in pixel coordinates.
        tw_xoff = int((tgw_ulx - t_geotransform[0]) / t_geotransform[1] + 0.1)
        tw_yoff = int((tgw_uly - t_geotransform[3]) / t_geotransform[5] + 0.1)
        tw_xsize = int((tgw_lrx - t_geotransform[0]) / t_geotransform[1] + 0.5) - tw_xoff
        tw_ysize = int((tgw_lry - t_geotransform[3]) / t_geotransform[5] + 0.5) - tw_yoff

        if tw_xsize < 1 or tw_ysize < 1:
            return 1

        # Compute source window in pixel coordinates.
        sw_xoff = int((tgw_ulx - self.geotransform[0]) / self.geotransform[1])
        sw_yoff = int((tgw_uly - self.geotransform[3]) / self.geotransform[5])
        sw_xsize = int((tgw_lrx - self.geotransform[0])
                       / self.geotransform[1] + 0.5) - sw_xoff
        sw_ysize = int((tgw_lry - self.geotransform[3])
                       / self.geotransform[5] + 0.5) - sw_yoff

        if sw_xsize < 1 or sw_ysize < 1:
            return 1

        # Open the source file, and copy the selected region.
        s_fh = gdal.Open(self.filename)

        return \
            raster_copy(s_fh, sw_xoff, sw_yoff, sw_xsize, sw_ysize, s_band,
                        t_fh, tw_xoff, tw_yoff, tw_xsize, tw_ysize, t_band,
                        nodata_arg)


class CreateMosaicGDAL:
    """A class to mosaic modis data from hdf to GDAL formats using GDAL

       :param list hdfnames: a list containing the name of tile to mosaic
       :param str subset: the subset of layer to consider
       :param str outformat: the output format to use, this parameter is
                             not used for the VRT output, supported values
                             are HDF4Image, GTiff, HFA, and maybe something
                             else not tested.
    """

    def __init__(self, hdf_names, subset=None, out_format="GTiff"):
        """Function for the initialize the object"""
        # Open source dataset
        self.in_names = hdf_names
        # #TODO use resolution into mosaic.
        # self.resolution = res

        if not subset:
            self.subset = None
        elif isinstance(subset, list):
            self.subset = subset
        elif isinstance(subset, str):
            self.subset = subset.replace('(', '').replace(')', '').strip().split()
        else:
            raise Exception('Type for subset parameter not supported')

        self.driver = gdal.GetDriverByName(out_format)
        self.out_format = out_format

        if self.driver is None:
            raise Exception('Format driver %s not found, pick a supported '
                            'driver.' % out_format)
        driver_metadata = self.driver.GetMetadata()

        if 'DCAP_CREATE' not in driver_metadata:
            raise Exception('Format driver %s does not support creation and'
                            ' piecewise writing.\nPlease select a format that'
                            ' does, such as GTiff (the default) or HFA (Erdas'
                            ' Imagine).' % format)
        self._init_layers()
        self._get_used_layers()
        self._names_to_file_infos()

    def _init_layers(self):
        """Set up the variable self.layers as dictionary for each chosen
        subset"""
        if isinstance(self.in_names, list):
            src_ds = gdal.Open(self.in_names[0])
        else:
            raise Exception("The input value should be a list of HDF files")
        layers = src_ds.GetSubDatasets()
        self.layers = OrderedDict()

        if not self.subset:
            self.subset = [1 for i in range(len(layers))]

        for i, sub in enumerate(self.subset):
            name = layers[i][0].split(':')[-1].strip('"')
            sub = str(sub)
            if sub == name or sub == '1':
                self.layers[name] = list()

    def _get_used_layers(self):
        """Add each subset to the correct list for each input layers"""
        for name in self.in_names:
            src_ds = gdal.Open(name)
            layers = src_ds.GetSubDatasets()

            for i, sub in enumerate(self.subset):
                name = layers[i][0].split(':')[-1].strip('"')
                sub = str(sub)

                if sub == name or sub == "1":
                    self.layers[name].append(layers[i][0])

    def _names_to_file_infos(self):
        """Translate a list of GDAL filenames, into file_info objects.
        Returns a list of file_info objects. There may be less file_info
        objects than names if some of the names could not be opened as GDAL
        files.
        """
        self.file_infos = OrderedDict()
        for k, v in self.layers.items():
            self.file_infos[k] = []
            for name in v:
                fi = FileInfo()
                if fi.init_from_name(name) == 1:
                    self.file_infos[k].append(fi)

    def _calculate_new_size(self):
        """Return the new size of output raster

           :return: X size, Y size and geotransform parameters
        """
        values = list(self.file_infos.values())
        l1 = values[0][0]
        ulx = l1.ulx
        uly = l1.uly
        lrx = l1.lrx
        lry = l1.lry
        for fi in self.file_infos[list(self.file_infos.keys())[0]]:
            ulx = min(ulx, fi.ulx)
            uly = max(uly, fi.uly)
            lrx = max(lrx, fi.lrx)
            lry = min(lry, fi.lry)
        p_size_x = l1.geotransform[1]
        p_size_y = l1.geotransform[5]

        geo_transform = [ulx, p_size_x, 0, uly, 0, p_size_y]
        x_size = int((lrx - ulx) / geo_transform[1] + 0.5)
        y_size = int((lry - uly) / geo_transform[5] + 0.5)
        return x_size, y_size, geo_transform

    def run(self, output, quiet=False, dst_srs=None):
        """Create the mosaic
           :param str output: the name of output file
        """
        values = list(self.file_infos.values())
        l1 = values[0][0]
        x_size, y_size, geo_transform = self._calculate_new_size()

        if dst_srs:
            output_file = tempfile.NamedTemporaryFile(delete=False).name
        else:
            output_file = output

        t_fh = self.driver.Create(output_file, x_size, y_size,
                                  len(list(self.file_infos.keys())),
                                  l1.band_type)
        if t_fh is None:
            raise Exception('Not possible to create dataset %s' % output)

        t_fh.SetGeoTransform(geo_transform)
        t_fh.SetProjection(l1.projection)
        i = 1
        for names in list(self.file_infos.values()):
            fill = None
            if names[0].fill_value:
                fill = float(names[0].fill_value)
                t_fh.GetRasterBand(i).SetNoDataValue(fill)
                t_fh.GetRasterBand(i).Fill(fill)
            for n in names:
                n.copy_into(t_fh, 1, i, fill)
            i = i + 1
        # self.write_mosaic_xml(output)

        if dst_srs:
            kwargs = {'format': self.out_format, 'dstSRS': dst_srs}
            gdal.Warp(destNameOrDestDS=output, srcDSOrSrcDSTab=t_fh, **kwargs)

        t_fh = None

        if not quiet:
            print("The mosaic file {name} has been "
                  "created".format(name=output))
        return True

    def _calculate_off_set(self, fileinfo, geo_transform):
        """Return the offset between main origin and the origin of current
        file

        :param fileinfo: a file_info object
        :param geotransform: the geotransform parameters to keep x and y origin
        """
        x = abs(int((geo_transform[0] - fileinfo.ulx) / geo_transform[1]))
        y = abs(int((geo_transform[3] - fileinfo.uly) / geo_transform[5]))
        return x, y

    def write_vrt(self, output, separate=True, quiet=False):
        """Write VRT file

        :param str output: the prefix of output file
        :param bool separate: True to write a VRT file for each band, False to
                              write an unique file
        """

        def write_complex(f, geot, band=1):
            """Write a complex source to VRT file"""
            out.write('\t\t<ComplexSource>\n')
            out.write('\t\t\t<SourceFilename relativeToVRT="0">{name}'
                      '</SourceFilename>\n'.format(name=f.filename.replace('"', '')))
            out.write('\t\t\t<SourceBand>{nb}</SourceBand>\n'.format(nb=band))
            out.write('\t\t\t<SourceProperties RasterXSize="{x}" '
                      'RasterYSize="{y}" DataType="{typ}" '
                      'BlockXSize="{bx}" BlockYSize="{by}" />'
                      '\n'.format(x=f.xsize, y=f.ysize,
                                  typ=gdal.GetDataTypeName(f.band_type),
                                  bx=f.block_size[0], by=f.block_size[1]))
            out.write('\t\t\t<SrcRect xOff="0" yOff="0" xSize="{x}" '
                      'ySize="{y}" />\n'.format(x=f.xsize, y=f.ysize))
            x_off, y_off = self._calculate_off_set(f, geot)
            out.write('\t\t\t<DstRect xOff="{xoff}" yOff="{yoff}" '
                      'xSize="{x}" ySize="{y}" />'
                      '\n'.format(xoff=x_off, yoff=y_off, x=f.xsize,
                                  y=f.ysize))
            if l1.fill_value:
                out.write('\t\t\t<NODATA>{va}</NODATA>'
                          '\n'.format(va=f.fill_value))
            out.write('\t\t</ComplexSource>\n')

        x_size, y_size, geot = self._calculate_new_size()
        if separate:
            for k in list(self.file_infos.keys()):
                l1 = self.file_infos[k][0]
                out = open("{pref}_{band}.vrt".format(pref=output, band=k),
                           'w')
                out.write('<VRTDataset rasterXSize="{x}" rasterYSize="{y}">'
                          '\n'.format(x=x_size, y=y_size))
                out.write('\t<SRS>{proj}</SRS>\n'.format(proj=l1.projection))
                out.write('\t<GeoTransform>{geo0}, {geo1}, {geo2}, {geo3},'
                          ' {geo4}, {geo5}</GeoTransform>'
                          '\n'.format(geo0=geot[0], geo1=geot[1], geo2=geot[2],
                                      geo3=geot[3], geo4=geot[4],
                                      geo5=geot[5]))
                gtype = gdal.GetDataTypeName(l1.band_type)
                # count max number of band
                n_bands = 0
                for f in self.file_infos[k]:
                    n_bands = max(n_bands, f.bands)
                for b in range(n_bands):
                    out.write('\t<VRTRasterBand dataType="{typ}" band="{band}"'
                              '>\n'.format(typ=gtype, band=b + 1))
                    if l1.fill_value:
                        out.write('\t\t<NoDataValue>{val}</NoDataValue'
                                  '>\n'.format(val=l1.fill_value))
                    out.write('<ColorInterp>Gray</ColorInterp>\n')
                    for f in self.file_infos[k]:
                        if b < f.bands:
                            write_complex(f, geot, band=b + 1)
                    out.write('\t</VRTRasterBand>\n')
                out.write('</VRTDataset>\n')
                out.close()
        else:
            values = list(self.file_infos.values())
            l1 = values[0][0]
            band = 1  # the number of band
            out = open("{pref}.vrt".format(pref=output), 'w')
            out.write('<VRTDataset rasterXSize="{x}" rasterYSize="{y}">'
                      '\n'.format(x=x_size, y=y_size))
            out.write('\t<SRS>{proj}</SRS>\n'.format(proj=l1.projection))
            out.write('\t<GeoTransform>{geo0}, {geo1}, {geo2}, {geo3},'
                      ' {geo4}, {geo5}</GeoTransform>\n'.format(geo0=geot[0],
                                                                geo1=geot[1], geo2=geot[2], geo3=geot[3], geo4=geot[4],
                                                                geo5=geot[5]))
            for k in list(self.file_infos.keys()):
                l1 = self.file_infos[k][0]
                out.write('\t<VRTRasterBand dataType="{typ}" band="{n}"'
                          '>\n'.format(typ=gdal.GetDataTypeName(l1.band_type),
                                       n=band))
                if l1.fill_value:
                    out.write('\t\t<NoDataValue>{val}</NoDataValue>\n'.format(
                        val=l1.fill_value))
                out.write('\t\t<ColorInterp>Gray</ColorInterp>\n')
                for f in self.file_infos[k]:
                    write_complex(f, geot)
                out.write('\t</VRTRasterBand>\n')
                band += 1
            out.write('</VRTDataset>\n')
            out.close()
        if not quiet:
            print("The VRT mosaic file {name} has been "
                  "created".format(name=output))
        return True

# from ingest.modis.convertmodis import CreateMosaicGDAL
# hdf_names = ["/gskydata/modis-data/MYD13Q1.A2023057.h17v08.061.2023074085220.hdf","/gskydata/modis-data/MYD13Q1.A2023057.h18v08.061.2023074071640.hdf","/gskydata/modis-data/MYD13Q1.A2023057.h18v09.061.2023074091534.hdf"]
# ms = CreateMosaicGDAL(hdf_names,["250m 16 days NDVI"])
