import json
import re
import pandas as pd
import mysql.connector.pooling


class Field:
    def __init__(self, field_name, type, nullable, is_key, default_value, extra):
        self.field_name = field_name
        self.type = type
        self.nullable = nullable
        self.is_key = is_key
        self.default_value = default_value
        self.extra = extra

    def __repr__(self):
        return f"Field(field_name='{self.field_name}', type='{self.type}', nullable='{self.nullable}', is_key='{self.is_key}', default_value='{self.default_value}', extra='{self.extra}')"


class Desc:
    def __init__(self, fields: list[Field]):
        self.fields = fields

    @classmethod
    def load(cls, desc):
        fields = []
        for row in desc:
            field = Field(*row)
            fields.append(field)

        return cls(fields)

    def __repr__(self):
        return f"Desc(fields={self.fields})"

    @classmethod
    def dataframe_convert(cls, df: pd.DataFrame):
        desc = []
        column_types = df.dtypes
        for column_name, column_type in column_types.items():
            print(f"Column Name:{column_name} || Column Type: {column_type}")
            if column_type == "object":
                desc.append((column_name, FieldType.STRING, None, None, None, None))
            elif column_type == "int64":
                desc.append((column_name, FieldType.BIGINT, None, None, None, None))
            elif column_type == "float64":
                desc.append((column_name, FieldType.DOUBLE, None, None, None, None))
            elif column_type == "datetime64[ns]":
                desc.append((column_name, FieldType.DATETIME, None, None, None, None))
            elif column_type == "bool":
                desc.append((column_name, FieldType.BOOLEAN, None, None, None, None))
            else:
                desc.append((column_name, FieldType.STRING, None, None, None, None))

        print(desc)
        return Desc.load(desc)

    def replace_field_type(self, mapping: list[tuple]):
        for field in mapping:
            field_name = field[0]
            field_type = field[1]
            for desc_field in self.fields:
                if field_name == desc_field.field_name:
                    desc_field.type = field_type
        return self


class FieldType:
    BIGINT = "BIGINT"
    STRING = "STRING"
    DATETIME = "DATETIME"
    BOOLEAN = "Boolean"
    DOUBLE = "Double"
    VARCHAR = "VARCHAR(255)"
    allow_field_types = ["BIGINT", "STRING", "DATETIME", "DATETIME(6)", "Boolean", "Double", "VARCHAR(255)"]


class TableModel:
    UNIQUE = "UNIQUE"
    AGGREGATE = "AGGREGATE"
    DUPLICATE = "DUPLICATE"


class Table:
    def __init__(self, df: pd.DataFrame, table_name, table_model: str, table_model_key: list = None,
                 distributed_hash_key: list = None, buckets=None, properties=None,
                 fields_mapping: list[tuple] = None):
        self.table_name = table_name
        self.fields_mapping = fields_mapping
        first_desc = Desc.dataframe_convert(df)
        if fields_mapping is not None:
            self.desc = first_desc.replace_field_type(mapping=fields_mapping)
        else:
            self.desc = first_desc
        self.table_model = table_model
        self.table_model_key = ",".join(table_model_key) if table_model_key is not None else self.gen_key_default()
        self.distributed_hash_key = ",".join(
            distributed_hash_key) if distributed_hash_key is not None else self.gen_key_default()
        self.buckets = buckets if buckets is not None else "AUTO"
        self.table_properties = self.gen_table_properties(properties)

    # use first field
    def gen_key_default(self):
        key = self.desc.fields[0].field_name
        if self.desc.fields[0].type == FieldType.STRING:
            self.desc.fields[0].type = FieldType.VARCHAR
        return key

    def gen_table_fields_info(self):
        fields = []
        for field in self.desc.fields:
            f_name = field.field_name
            f_type = field.type
            f_nullable = field.nullable if field.nullable is not None else "NULL"
            fields.append(f"{f_name} {f_type} {f_nullable}")
        table_fields_info = ",\n".join(fields)
        return table_fields_info

    def gen_distribute_info(self):
        return f"DISTRIBUTED BY HASH ({self.distributed_hash_key}) BUCKETS {self.buckets}"

    def gen_table_properties(self, properties):
        if properties is None:
            prop = {"replication_allocation": "tag.location.default: 1"}
            table_properties = json.dumps(prop, separators=(",", "="))[1:-1]
        else:
            table_properties = json.dumps(properties, separators=(",", "="))[1:-1]
        return table_properties

    def gen_create_table_sql(self, replace_table):
        replace_str = " IF NOT EXISTS "
        # if replace_table:
        #     replace_str = " IF NOT EXISTS "
        table_fields_info = self.gen_table_fields_info()
        distributed = self.gen_distribute_info()
        if self.table_properties == '':
            sql = f"CREATE TABLE {replace_str}{self.table_name} (\n{table_fields_info}) \n{self.table_model} KEY({self.table_model_key})\n{distributed}"
            return sql
        return f"CREATE TABLE {replace_str}{self.table_name} (\n{table_fields_info}) \n{self.table_model} KEY({self.table_model_key})\n{distributed}\nPROPERTIES({self.table_properties})"


class SelectDBBase:
    def __init__(self, host, port, database, user, password, pool_size):
        self.host = host
        self.port = port
        self.db = database
        self.user = user
        self.passwd = password
        self.pool_size = pool_size
        self.pool = self.create_pool()

    def create_pool(self, pool_name="pydoris_pool"):
        pool = mysql.connector.pooling.MySQLConnectionPool(pool_name=pool_name,
                                                           pool_size=self.pool_size,
                                                           host=self.host,
                                                           port=self.port,
                                                           user=self.user,
                                                           password=self.passwd,
                                                           database=self.db,
                                                           pool_reset_session=False)
        return pool

    def close(self, cursor, conn):
        cursor.close()
        conn.close()

    def query(self, sql):
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        self.close(conn, cursor)
        return res

    def execute(self, sql):
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        self.close(conn, cursor)

    def create_database(self, database):
        self.execute(f"create database if not exists {database}")


    def get_create_table_expr(self, table_name):
        data = self.query(f"show create table {table_name}")
        return data[0][1]

    def drop_table(self, table_name):
        self.execute(f"drop table if exists {table_name}")

    def get_table_properties(self, table_name):
        result = self.query(f"show create table {table_name}")
        data = result[0][1]
        properties_match = re.search(r"PROPERTIES\s*\((.+)\)", data, re.DOTALL)
        if properties_match:
            properties_str = properties_match.group(1)
            properties_str = re.sub(r"(\w+)\s*=", r'"\1":', properties_str)
            properties_str = re.sub(r"\s+", "", properties_str)
            properties_dict = json.loads("{" + properties_str.replace('=', ':') + "}")
            return properties_dict

    def get_table_columns(self, table):
        # cursor = self.conn.cursor()
        desc = self.query(f"SHOW COLUMNS FROM {table}")
        # desc = cursor.fetchall()
        return Desc.load(desc)

    def get_table_fields_list(self, table):
        desc = self.query(f"SHOW COLUMNS FROM {table}")
        f_list = []
        for field in desc:
            f_list.append(field[0])

        return f_list

    def create_table_from_df(self, replace_table, data_df: pd.DataFrame, table_name: str, table_model: str,
                             table_module_key=None,
                             distributed_hash_key=None,
                             buckets=None,
                             table_properties=None,
                             field_mapping: list[tuple] = None,
                             ):
        table = Table(
            data_df,
            table_name,
            table_model,
            table_model_key=table_module_key,
            distributed_hash_key=distributed_hash_key,
            buckets=buckets,
            properties=table_properties,
            fields_mapping=field_mapping
        )
        sql = table.gen_create_table_sql(replace_table)
        print(sql)
        self.execute(sql)

    def read_to_df(self, sql, columns: list):
        result = self.query(sql)
        return pd.DataFrame(result, columns=columns)

    def get_tables(self, db):
        result = self.query(f"show tables from {db}")
        return result
