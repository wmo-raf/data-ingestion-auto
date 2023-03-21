import json
from datetime import datetime

import psycopg2
from geojson import loads
from psycopg2.extras import execute_values
from shapely.geometry import LineString
from shapely.geometry import shape

from ingest.errors import UnKnownGeomType


def fix_linestring_within_world_extents(linestring):
    def fix_coordinates(coords):
        def clamp(val, min_val, max_val):
            return max(min(val, max_val), min_val)

        fixed_coords = []
        for lon, lat in coords:
            fixed_lon = clamp(lon, -180, 180)
            fixed_lat = clamp(lat, -90, 90)
            fixed_coords.append((fixed_lon, fixed_lat))
        return fixed_coords

    if not linestring.is_simple:
        linestring = linestring.simplify(tolerance=0.001, preserve_topology=True)
    fixed_line_coords = fix_coordinates(linestring.coords)
    return LineString(fixed_line_coords)


def is_valid_geom_type(geom_type):
    allowed_geom_types = ["Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon"]

    return geom_type in allowed_geom_types


class VectorDbManager:
    def __init__(self, conn_params, schema_name, table_name, geom_type, data_columns, srid=3857, delete_past_data=True):
        self.conn_params = conn_params

        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.schema_name}.{self.table_name}"

        if is_valid_geom_type(geom_type):
            self.geom_type = geom_type
        else:
            raise UnKnownGeomType(f"Unknown geom type: {geom_type}")

        self.srid = srid

        self.data_columns = data_columns
        self.delete_past_data = delete_past_data

        # initialize db
        self.enable_postgis_extension()
        self.create_schema_if_not_exists()
        self.create_table_if_not_exists()
        self.create_or_replace_mvt_function()

    def enable_postgis_extension(self):
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    def create_schema_if_not_exists(self):
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")

    def create_table_if_not_exists(self):
        data_columns_sql = ', '.join([f"{column_name} REAL" for column_name in self.data_columns])
        data_columns_sql = f', {data_columns_sql}'

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f'''CREATE TABLE IF NOT EXISTS {self.full_table_name}
                               (id SERIAL PRIMARY KEY,
                                date TIMESTAMP,
                                geom GEOMETRY({self.geom_type}, {self.srid}){data_columns_sql})''')
                cur.execute(f"CREATE INDEX IF NOT EXISTS {self.table_name}_date_idx ON {self.full_table_name}(date)")

    def create_or_replace_mvt_function(self):
        # Prepare the additional_columns string
        additional_columns_str = ', '.join([f"t.{col}" for col in self.data_columns])

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                # Create the dynamic SQL string
                sql = f"""
                CREATE OR REPLACE
                FUNCTION {self.schema_name}.{self.table_name}(
                            z integer, x integer, y integer,
                            data_date timestamp)
                RETURNS bytea
                AS $$
                    WITH
                    bounds AS (
                      SELECT ST_TileEnvelope(z, x, y) AS geom
                    ),
                    mvtgeom AS (
                      SELECT ST_AsMVTGeom(ST_Transform(t.geom, 3857), bounds.geom) AS geom,
                        t.date, {additional_columns_str}
                      FROM {self.full_table_name} t, bounds
                      WHERE ST_Intersects(t.geom, ST_Transform(bounds.geom, 4326))
                      AND t.date = data_date
                    )
                    SELECT ST_AsMVT(mvtgeom, 'default') FROM mvtgeom;
                $$
                LANGUAGE 'sql'
                STABLE
                PARALLEL SAFE;
                """

                cur.execute(sql)

    def process_geojson(self, date_str, data_file):
        with open(data_file) as f:
            data = json.load(f)

        rows = []
        for feature in data['features']:
            date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            geometry = loads(json.dumps(feature['geometry']))
            geom_type = geometry.get("type")

            if geom_type != self.geom_type:
                raise UnKnownGeomType(
                    f"GeomType from feature {geom_type} is different from table geom type: {self.geom_type}")

            shapely_geom = shape(geometry)

            if geom_type == "LineString":
                geom = fix_linestring_within_world_extents(shapely_geom)
                shapely_geom = shape(geom)

            custom_columns_values = []
            for column_name in self.data_columns:
                col_value = feature['properties'][column_name]
                custom_columns_values.append(col_value)

            rows.append((date, shapely_geom.wkt, *custom_columns_values,))

        return rows

    def insert_update_data(self, date_str, data_file, latest_date_str=None):
        rows = self.process_geojson(date_str, data_file)
        columns = ["date", "geom", *self.data_columns]
        column_names = ', '.join(columns)

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.full_table_name} WHERE date = '{date_str}'")
                count = cur.fetchone()[0]

                if count > 0:
                    cur.execute(f"DELETE FROM {self.full_table_name} WHERE date = '{date_str}'")

                sql = f"INSERT INTO {self.full_table_name} ({column_names}) VALUES %s"

                execute_values(cur, sql, rows)

                if self.delete_past_data and latest_date_str:
                    cur.execute(f"DELETE FROM {self.full_table_name} WHERE date < '{latest_date_str}'")
