# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=47


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
        + config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_HuShenGangTongZiJinLiuXiang"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期",
            ggt_ss VARCHAR(50) COMMENT "港股通（上海）",
            ggt_sz VARCHAR(50) COMMENT "港股通（深圳）",
            hgt VARCHAR(50) COMMENT "沪股通",
            sgt VARCHAR(50) COMMENT "深股通",
            north_money VARCHAR(50) COMMENT "北向资金",
            south_money VARCHAR(50) COMMENT "南向资金"          
        )
        UNIQUE KEY(trade_date)
        COMMENT "tushare_沪深股票_资金流向数据_沪深港通资金流向"

        DISTRIBUTED BY HASH(trade_date) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


# 从tushare下载
def download_from_tushare(
    pro,
    trade_date="",
    start_date="",
    end_date="",
    limit="",
    offset="",
):
    df_local = pro.moneyflow_hsgt(
        **{
            "trade_date": trade_date,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "offset": offset,
        },
        fields=["trade_date", "ggt_ss", "ggt_sz", "hgt", "sgt", "north_money", "south_money"]
    )
    return df_local


def download():
    """下载 沪深股票_资金流向数据_沪深港通资金流向"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_HuShenGangTongZiJinLiuXiang"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按开始、结束日期下载
    basis.basis_function.download_by_start_end_date(table_name, download_from_tushare, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
