from pydoris.doris_client import *
from pydoris.util.generate_test_data import *

import basis.basis_function


def connect_doris():
    """
    此处DorisClient的参数db并未指定具体数据库
    """
    config = basis.basis_function.load_config()
    # 连接database
    doris_client = DorisClient(
        fe_host=config["host"],
        fe_query_port=config["fe"]["query_port"],
        fe_http_port=config["fe"]["http_port"],
        username=config["user_name"],
        password=config["password"],
        db="",
    )
    return doris_client


def connect_database():
    config = basis.basis_function.load_config()
    # 连接database
    doris_client = DorisClient(
        fe_host=config["host"],
        fe_query_port=config["fe"]["query_port"],
        fe_http_port=config["fe"]["http_port"],
        username=config["user_name"],
        password=config["password"],
        db=config["database_name"],
    )
    return doris_client


# https://juejin.cn/post/7262515500336054328
# https://pypi.org/project/pydoris-client/
def upload_dataframe_as_csv(dataframe, table_name):
    config = basis.basis_function.load_config()
    doris_client = connect_database()
    dataframe.to_csv(
        "/Tushare2Doris/test.csv", header=False, index=False, na_rep=""
    )  # 但dataframe转换为csv，以csv格式上传
    csv = dataframe.to_csv(header=False, index=False, na_rep="")  # 但dataframe转换为csv，以csv格式上传
    doris_client.write(config["database_name"] + "." + table_name, csv)


def upload_dataframe_as_json(dataframe, table_name, logger):
    config = basis.basis_function.load_config()
    doris_client = connect_database()
    json_data = dataframe.to_json(orient="records")
    # 如果tushare返回的结果为空，此时json_data为'[]'，这种情况无需入库
    if json_data != "[]":  #
        options = WriteOptions()
        options.set_json_format()
        options.set_option("strip_outer_array", "true")
        doris_client.write(config["database_name"] + "." + table_name, json_data, logger, options=options)
    if json_data == "[]":
        logger.info(table_name + " 待上传数据为空")
