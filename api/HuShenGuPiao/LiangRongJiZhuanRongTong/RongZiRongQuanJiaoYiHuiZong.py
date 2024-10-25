# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=58


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
        + config["Ts_HuShenGuPiao_LiangRongJiZhuanRongTong_RongZiRongQuanJiaoYiHuiZong"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期",
            exchange_id VARCHAR(50) COMMENT "交易所代码（SSE上交所SZSE深交所）",
            rzye  DECIMAL COMMENT "融资余额(元)",
            rzmre  DECIMAL COMMENT "融资买入额(元)",
            rzche  DECIMAL COMMENT "融资偿还额(元)",
            rqye  DECIMAL COMMENT "融券余额(元)",
            rqmcl  DECIMAL COMMENT "融券卖出量(股,份,手)",
            rzrqye  DECIMAL COMMENT "融资融券余额(元)",
            rqyl  DECIMAL COMMENT "融券余量"           
        )
        UNIQUE KEY(trade_date,exchange_id)
        COMMENT "tushare_沪深股票_两融及转融通_融资融券交易汇总"

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
    exchange_id="",
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
        df = pro.margin(
            **{
                "trade_date": trade_date,
                "exchange_id": exchange_id,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "offset": offset,
            },
            fields=["trade_date", "exchange_id", "rzye", "rzmre", "rzche", "rqye", "rqmcl", "rzrqye", "rqyl"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_两融及转融通_融资融券交易汇总"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_LiangRongJiZhuanRongTong_RongZiRongQuanJiaoYiHuiZong"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
