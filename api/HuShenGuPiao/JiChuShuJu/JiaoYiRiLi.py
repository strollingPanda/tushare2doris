# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time


# https://tushare.pro/document/2?doc_id=138


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_JiChuShuJu_JiaoYiRiLi"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_HuShenGuPiao_JiChuShuJu_JiaoYiRiLi"]["table_name"]
        + """
        (
            exchange VARCHAR(50) COMMENT "交易所 SSE上交所 SZSE深交所",
            cal_date VARCHAR(8) COMMENT "日历日期",
            is_open VARCHAR(1) COMMENT "是否交易 0休市 1交易",
            pretrade_date VARCHAR(8) COMMENT "上一个交易日",
        )
        DUPLICATE KEY(exchange,cal_date)
        COMMENT "tushare_沪深股票_基础数据_交易日历"

        DISTRIBUTED BY HASH(exchange) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro,
    logger,
    exchange="",
    cal_date="",
    start_date="",
    end_date="",
    is_open="",
    limit="",
    offset="",
):
    """
    执行具体下载操作
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
    """下载 沪深股票_基础数据_交易日历"""
    exchange_all = {
        "SSE": "上交所",
        "SZSE": "深交所",
    }
    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_JiChuShuJu_JiaoYiRiLi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 下载每一个交易所数据
    for exchange_local in exchange_all.keys():
        logger.info("downloading " + table_name + ". exchange: " + exchange_local)
        df = download_execute(pro, logger, exchange=exchange_local)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
