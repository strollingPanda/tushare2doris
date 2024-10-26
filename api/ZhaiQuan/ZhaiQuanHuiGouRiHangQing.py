# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import basis.with_pydoris
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=256


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
        + config["Ts_ZhaiQuan_ZhaiQuanHuiGouRiHangQing"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            trade_date VARCHAR(50) COMMENT "交易日期",
            repo_maturity VARCHAR(50) COMMENT "期限品种",
            pre_close DECIMAL COMMENT "前收盘(%)",
            open DECIMAL COMMENT "开盘价(%)",
            high DECIMAL COMMENT "最高价(%)",
            low DECIMAL COMMENT "最低价(%)",
            close DECIMAL COMMENT "收盘价(%)",
            weight DECIMAL COMMENT "加权价(%)",
            weight_r DECIMAL COMMENT "加权价(利率债)(%)",
            amount DECIMAL COMMENT "成交金额(万元)",
            num BIGINT COMMENT "成交笔数(笔)"
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_债券_债券回购日行情"

        DISTRIBUTED BY HASH(trade_date) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


# 从tushare下载
def download_from_tushare(
    pro,
    ts_code="",
    trade_date="",
    start_date="",
    end_date="",
    limit="",
    offset="",
):
    df_local = pro.repo_daily(
        **{
            "ts_code": ts_code,
            "trade_date": trade_date,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "offset": offset,
        },
        fields=[
            "ts_code",
            "trade_date",
            "repo_maturity",
            "pre_close",
            "open",
            "high",
            "low",
            "close",
            "weight",
            "weight_r",
            "amount",
            "num",
        ]
    )
    return df_local


def download():
    """下载 债券_债券回购日行情"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_ZhaiQuanHuiGouRiHangQing"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按开始、结束日期下载
    basis.basis_function.download_by_start_end_date(table_name, download_from_tushare, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
