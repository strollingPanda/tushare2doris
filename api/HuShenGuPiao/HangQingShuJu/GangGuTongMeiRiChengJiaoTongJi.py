# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=196


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
        + config["Ts_HuShenGuPiao_HangQingShuJu_GangGuTongMeiRiChengJiaoTongJi"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期",
            buy_amount DECIMAL COMMENT "买入成交金额（亿元）",
            buy_volume DECIMAL COMMENT "买入成交笔数（万笔）",
            sell_amount DECIMAL COMMENT "卖出成交金额（亿元）",
            sell_volume DECIMAL COMMENT "卖出成交笔数（万笔）",            
        )
        UNIQUE KEY(trade_date)
        COMMENT "tushare_沪深股票_行情数据_港股通每日成交统计"

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
    trade_date="",
    start_date="",
    end_date="",
    limit="",
    offset="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.ggt_daily(
            **{
                "trade_date": trade_date,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "offset": offset,
            },
            fields=["trade_date", "buy_amount", "buy_volume", "sell_amount", "sell_volume"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_行情数据_港股通每日成交统计"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_HangQingShuJu_GangGuTongMeiRiChengJiaoTongJi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
