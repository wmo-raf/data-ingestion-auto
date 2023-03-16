import json
from datetime import datetime

import psycopg2
from geojson import loads
from psycopg2.extras import execute_values
from shapely.geometry import shape


class RasterVectorDatabase:
    def __init__(self, database, user, password, host, port, schema_name, table_name, data_columns):
        self.conn_params = {
            'database': database,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }

        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{self.schema_name}.{self.table_name}"

        self.data_columns = data_columns

        # initialize db
        self.enable_postgis_extension()
        self.create_schema_if_not_exists()
        self.create_table_if_not_exists()

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
                                geom GEOMETRY{data_columns_sql})''')
                cur.execute(f"CREATE INDEX IF NOT EXISTS {self.table_name}_date_idx ON {self.full_table_name}(date)")

    def process_geojson(self, date_str, data_file):
        with open(data_file) as f:
            data = json.load(f)

        rows = []
        for feature in data['features']:
            date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            geometry = loads(json.dumps(feature['geometry']))
            shapely_geom = shape(geometry)

            custom_columns_values = []
            for column_name in self.data_columns:
                col_value = feature['properties'][column_name]
                custom_columns_values.append(col_value)

            rows.append((date, shapely_geom.wkt, *custom_columns_values,))

        return rows

    def insert_update_data(self, date_str, data_file):
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
