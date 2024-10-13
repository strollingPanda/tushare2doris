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
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=141


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
        + config["Ts_QiHuo_MeiRiJieSuanCanShu"]["table_name"]
        + """
        (
            ts_code VARCHAR(30) COMMENT "合约代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            settle DECIMAL COMMENT "结算价",
            trading_fee_rate DECIMAL COMMENT "交易手续费率",
            trading_fee DECIMAL COMMENT "交易手续费",
            delivery_fee DECIMAL COMMENT "交割手续费",
            b_hedging_margin_rate DECIMAL COMMENT "买套保交易保证金率",
            s_hedging_margin_rate DECIMAL COMMENT "卖套保交易保证金率",
            long_margin_rate DECIMAL COMMENT "买投机交易保证金率",
            short_margin_rate DECIMAL COMMENT "卖投机交易保证金率",
            offset_today_fee DECIMAL COMMENT "平今仓手续率",
            exchange VARCHAR(50) COMMENT "交易所",
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_期货_每日结算参数"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro, logger, trade_date="", ts_code="", start_date="", end_date="", exchange="", limit="", offset=""
):
    """执行具体下载操作"""
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.fut_settle(
            **{
                "trade_date": trade_date,
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date,
                "exchange": exchange,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "trade_date",
                "settle",
                "trading_fee_rate",
                "trading_fee",
                "delivery_fee",
                "b_hedging_margin_rate",
                "s_hedging_margin_rate",
                "long_margin_rate",
                "short_margin_rate",
                "offset_today_fee",
                "exchange",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 期货-每日结算参数"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_QiHuo_MeiRiJieSuanCanShu"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "期货", logger)


if __name__ == "__main__":
    create_table()
    download()
