# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=214


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
        + config["Ts_HuShenGuPiao_HangQingShuJu_MeiRiTingFuPaiXinXi"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            trade_date VARCHAR(8) COMMENT "停复牌日期",
            suspend_timing VARCHAR(200) COMMENT "日内停牌时间段",
            suspend_type VARCHAR(30) COMMENT "停复牌类型：S-停牌，R-复牌",            
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_沪深股票_行情数据_每日停复牌信息"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


# 从tushare下载
def download_from_tushare(
    pro,
    ts_code="",
    suspend_type="",
    trade_date="",
    start_date="",
    end_date="",
    limit="",
    offset="",
):
    df_local = pro.suspend_d(
        **{
            "ts_code": ts_code,
            "suspend_type": suspend_type,
            "trade_date": trade_date,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "offset": offset,
        },
        fields=["ts_code", "trade_date", "suspend_timing", "suspend_type"]
    )
    return df_local


def download():
    """下载 沪深股票_行情数据_每日停复牌信息"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_HangQingShuJu_MeiRiTingFuPaiXinXi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按开始、结束日期下载
    basis.basis_function.download_by_start_end_date(table_name, download_from_tushare, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
