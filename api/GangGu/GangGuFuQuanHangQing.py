# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=339


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
        + config["Ts_GangGu_GangGuFuQuanHangQing"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "股票代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            close DECIMAL COMMENT "收盘价",
            open DECIMAL COMMENT "开盘价",
            high DECIMAL COMMENT "最高价",
            low DECIMAL COMMENT "最低价",
            pre_close DECIMAL COMMENT "昨收价",
            change DECIMAL COMMENT "涨跌额",
            pct_change DECIMAL COMMENT "涨跌幅",
            vol DECIMAL COMMENT "成交量",
            amount DECIMAL COMMENT "成交额",
            vwap DECIMAL COMMENT "平均价",
            adj_factor DECIMAL COMMENT "复权因子",
            turnover_ratio DECIMAL COMMENT "换手率",
            free_share DECIMAL COMMENT "流通股本",
            total_share DECIMAL COMMENT "总股本",
            free_mv DECIMAL COMMENT "流通市值",
            total_mv DECIMAL COMMENT "总市值"
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_港股_港股复权行情"

        DISTRIBUTED BY HASH(trade_date) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro, logger, ts_code="", trade_date="", start_date="", end_date="", offset="", limit="", symbol=""
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.hk_daily_adj(
            **{
                "ts_code": ts_code,
                "trade_date": trade_date,
                "start_date": start_date,
                "end_date": end_date,
                "offset": offset,
                "limit": limit,
                "symbol": symbol,
            },
            fields=[
                "ts_code",
                "trade_date",
                "close",
                "open",
                "high",
                "low",
                "pre_close",
                "change",
                "pct_change",
                "vol",
                "amount",
                "vwap",
                "adj_factor",
                "turnover_ratio",
                "free_share",
                "total_share",
                "free_mv",
                "total_mv",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 港股_港股复权行情"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_GangGu_GangGuFuQuanHangQing"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "港股", logger)


if __name__ == "__main__":
    create_table()
    download()
