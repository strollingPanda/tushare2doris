# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=135


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_QiHuo_HeYueXinXi"]["table_name"]
        + """
    (
        ts_code	VARCHAR(30) COMMENT "合约代码", 
        symbol	VARCHAR(30) COMMENT "交易标识",
        exchange	VARCHAR(30) COMMENT "交易市场",
        name VARCHAR(100) COMMENT "中文简称",
        fut_code VARCHAR(30) COMMENT "合约产品代码",
        multiplier	FLOAT COMMENT "合约乘数",
        trade_unit	VARCHAR(30) COMMENT "交易计量单位",
        per_unit	FLOAT COMMENT "交易单位(每手)",
        quote_unit	VARCHAR(30) COMMENT "报价单位",
        quote_unit_desc	VARCHAR(30) COMMENT "最小报价单位说明",
        d_mode_desc	VARCHAR(30) COMMENT "交割方式说明",
        list_date	VARCHAR(8) COMMENT "上市日期",
        delist_date	VARCHAR(8) COMMENT "最后交易日期",
        d_month	VARCHAR(30) COMMENT "交割月份",
        last_ddate	VARCHAR(30) COMMENT "最后交割日",
        trade_time_desc	VARCHAR(300) COMMENT "交易时间说明"

    )
    UNIQUE KEY(ts_code)
    COMMENT "tushare-期货-合约信息"

    DISTRIBUTED BY HASH(ts_code) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, exchange="", fut_type="", ts_code="", fut_code="", limit="", offset=""):
    """执行具体下载操作"""
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.fut_basic(
            **{
                "exchange": exchange,
                "fut_type": fut_type,
                "ts_code": ts_code,
                "fut_code": fut_code,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "symbol",
                "exchange",
                "name",
                "fut_code",
                "multiplier",
                "trade_unit",
                "per_unit",
                "quote_unit",
                "quote_unit_desc",
                "d_mode_desc",
                "list_date",
                "delist_date",
                "d_month",
                "last_ddate",
                "trade_time_desc",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 期货-合约信息："""
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_QiHuo_HeYueXinXi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 在所有交易所，分别下载数据
    for exchange in config["Ts_QiHuo_exchange_list"]:
        logger.info("downloading " + table_name + ". exchange: " + exchange)
        df = download_execute(pro, logger, exchange=exchange)  # 下载数据
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
