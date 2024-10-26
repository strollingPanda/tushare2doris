# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=268


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
        + config["Ts_ZhiShu_ShenZhenShiChangMeiRiJiaoYiQingKuang"]["table_name"]
        + """
        (
            trade_date VARCHAR(8),
            ts_code	VARCHAR(50) COMMENT "市场类型",
            count BIGINT COMMENT "股票个数",
            amount DECIMAL COMMENT "成交金额",
            vol DECIMAL COMMENT "成交量",
            total_share DECIMAL COMMENT "总股本",
            total_mv DECIMAL COMMENT "总市值",
            float_share DECIMAL COMMENT "流通股票",
            float_mv DECIMAL COMMENT "流通市值"       
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_指数_深圳市场每日交易情况"

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
    df_local = pro.sz_daily_info(
        **{
            "trade_date": trade_date,
            "ts_code": ts_code,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "offset": offset,
        },
        fields=[
            "trade_date",
            "ts_code",
            "count",
            "amount",
            "vol",
            "total_share",
            "total_mv",
            "float_share",
            "float_mv",
        ]
    )
    return df_local


def download():
    """下载 指数_深圳市场每日交易情况"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_ShenZhenShiChangMeiRiJiaoYiQingKuang"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按开始、结束日期下载
    basis.basis_function.download_by_start_end_date(table_name, download_from_tushare, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
