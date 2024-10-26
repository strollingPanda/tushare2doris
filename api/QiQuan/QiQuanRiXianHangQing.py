# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=159


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
        + config["Ts_QiQuan_QiQuanRiXianHangQing"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            exchange VARCHAR(50) COMMENT "交易市场",
            pre_settle DECIMAL COMMENT "昨结算价",
            pre_close DECIMAL COMMENT "前收盘价",
            open DECIMAL COMMENT "开盘价",
            high DECIMAL COMMENT "最高价",
            low DECIMAL COMMENT "最低价",
            close DECIMAL COMMENT "收盘价",
            settle DECIMAL COMMENT "结算价",
            vol DECIMAL COMMENT "成交量(手)",
            amount DECIMAL COMMENT "成交金额(万元)",
            oi DECIMAL COMMENT "持仓量(手)"
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_期权_期权日线行情"

        DISTRIBUTED BY HASH(trade_date) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro,
    logger,
    ts_code="",
    trade_date="",
    start_date="",
    end_date="",
    exchange="",
    limit=1000,
    offset="",
):
    """
    执行具体下载操作
    """
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pandas.DataFrame()  # 最终函数的返回值
        df_local = pandas.DataFrame()  # 暂时设为空值，以执行循环
        num_times_download = 0  # 已经下载了几次
        # 若首次下载，或下载的数据大于等于limit，继续循环
        while num_times_download == 0 or df_local.shape[0] >= limit:
            df_local = pro.opt_daily(
                **{
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "exchange": exchange,
                    "limit": limit,
                    "offset": offset,
                },
                fields=[
                    "ts_code",
                    "trade_date",
                    "exchange",
                    "pre_settle",
                    "pre_close",
                    "open",
                    "high",
                    "low",
                    "close",
                    "settle",
                    "vol",
                    "amount",
                    "oi",
                ]
            )  # 从tushare下载
            num_times_download += 1  # 已经下载了几次
            df = pandas.concat([df, df_local])  # 将df_local加入结果
            offset = num_times_download * limit  # 计算新的offset
            time.sleep(config["regular_gap"])  # 等待一段时间再继续下载
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 期权_期权日线行情"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_QiQuan_QiQuanRiXianHangQing"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
