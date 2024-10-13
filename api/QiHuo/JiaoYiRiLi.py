# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time
import basis.with_pydoris
import basis.basis_function
import datetime
import pandas
import time

# https://tushare.pro/document/2?doc_id=137


# 创建表格
def create_table():
    """
    创建表格
    """
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_QiHuo_JiaoYiRiLi"]["table_name"]
        + """
    (
        exchange VARCHAR(50) COMMENT "交易所",
        cal_date VARCHAR(8) COMMENT "日历日期",
        is_open BIGINT COMMENT "是否交易 0休市 1交易",
        pretrade_date VARCHAR(8) COMMENT "上一个交易日",
    )
    UNIQUE KEY(exchange,cal_date)
    COMMENT "tushare-期货-JiaoYiRiLi"

    DISTRIBUTED BY HASH(exchange) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro, logger, exchange="", cal_date="", start_date="", end_date="", is_open="", limit="", offset=""
):
    """
    执行具体的下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况

    try:
        df = pro.trade_cal(
            **{
                "exchange": exchange,
                "cal_date": cal_date,
                "start_date": start_date,
                "end_date": end_date,
                "is_open": is_open,
                "limit": limit,
                "offset": offset,
            },
            fields=["exchange", "cal_date", "is_open", "pretrade_date"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 期货-交易日历"""
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_QiHuo_JiaoYiRiLi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 在所有交易所，分别下载数据
    for exchange in config["Ts_QiHuo_exchange_list"]:
        logger.info("downloading " + table_name + ". exchange: " + exchange)
        # 下载的end_date用当前日期
        end_date_local = datetime.datetime.today().strftime("%Y%m%d")
        df = download_execute(pro, logger, end_date=end_date_local)
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 将下载的数据上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
