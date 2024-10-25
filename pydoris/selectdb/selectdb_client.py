from stage_load import SelectDBStageLoad
from selectdb_utils import DelimiterParser
from pydoris.selectdb.db_operator import *
import pydoris.doris_client as dc


class SelectDBCloudClient:
    def __init__(self):
        self.options: SelectDBOptions = SelectDBOptions()

    def query(self, sql):
        return self.options.db_operator.query(sql)

    def execute(self, sql):
        self.options.db_operator.execute(sql)

    def query_to_dataframe(self, sql, columns: list):
        return self.options.db_operator.read_to_df(sql, columns)

    def write_from_df(self, data_df: pd.DataFrame, db, table_name: str, table_model: str,
                      table_module_key=None,
                      distributed_hash_key=None,
                      buckets=None,
                      table_properties=None,
                      field_mapping: list[tuple] = None):
        tables = self.list_tables(db)
        exists = table_name in tables
        if not exists:
            self.options.db_operator.create_table_from_df(data_df, f"{db}.{table_name}", table_model,
                                                          table_module_key,
                                                          distributed_hash_key,
                                                          buckets,
                                                          table_properties,
                                                          field_mapping
                                                          )
        data_list = list(data_df.values)
        self.stage_load(data_list, db, table_name, list(data_df))

    def list_tables(self, database):
        list_tuple = self.options.db_operator.get_tables(database)
        return [t[0] for t in list_tuple]

    def drop_table(self, db, table_name):
        return self.options.db_operator.drop_table(f"{db}.{table_name}")

    def create_database(self, database):
        return self.options.db_operator.create_database(database)

    def get_table_columns(self, db, table_name):
        return self.options.db_operator.get_table_columns(f"{db}.{table_name}")

    def stage_load(self, data: list[list], db, table, columns=None):
        copy_into_process = SelectDBStageLoad(self.options)
        copy_into_process.load_list(data, db, table, columns)

    def close(self):
        return self.options.db_operator.close()


class SelectDBOptions:
    def __init__(self):
        self.db_operator: SelectDBBase = None
        self.fe_host = None
        self.fe_http_port = None
        self.fe_query_port = None
        self.username = None
        self.password = None
        self.jar_path = None
        self.db = None
        self.cluster_name = None
        self.file_split_size = 2 * 1024 * 1024 * 1024
        self.row_batch_size = 10000000
        self.copy_into_props = {"file.type": "json",
                                "file.strip_outer_array": "true",
                                "copy.async": "false",
                                "copy.strict_mode": "false"}

    def set_copy_into_props(self, properties: dict):
        merged_dict = self.copy_into_props.copy()
        merged_dict.update(properties)
        self.copy_into_props = merged_dict

    def set_copy_into_file_type(self, file_type: str):
        self.copy_into_props['file.type'] = file_type

    def set_copy_into_file_column_separator(self, separator):
        self.copy_into_props['file.column_separator'] = DelimiterParser.parse(separator, '\t')

    def set_copy_into_file_line_delimiter(self, delimiter):
        self.copy_into_props['file.line_delimiter'] = DelimiterParser.parse(delimiter, '\n')

    def set_copy_into_strict_mode(self, strict: str):
        self.copy_into_props['copy.strict_mode'] = strict

    def get_copy_into_props(self):
        return self.copy_into_props

    def create_selectdb_cloud_operator(self, cluster=""):
        if self.jar_path is not None:
            self.db_operator = SelectDBBase(f"{self.fe_host}:{self.fe_query_port}",
                                            f"{self.db}@{cluster}" if cluster != "" else self.db, self.username,
                                            self.password,
                                            self.jar_path)
        else:
            print("create db operator failed , please add jar_path in options")
