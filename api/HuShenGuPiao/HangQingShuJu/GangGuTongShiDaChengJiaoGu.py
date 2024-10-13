# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=49


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
        + config["Ts_HuShenGuPiao_HangQingShuJu_GangGuTongShiDaChengJiaoGu"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期",
            ts_code VARCHAR(50) COMMENT "股票代码",
            name VARCHAR(30) COMMENT "股票名称",
            close DECIMAL COMMENT "收盘价",
            p_change DECIMAL COMMENT "涨跌幅",
            rank VARCHAR(30) COMMENT "资金排名",
            market_type VARCHAR(30) COMMENT "市场类型 2：港股通（沪） 4：港股通（深）",
            amount DECIMAL COMMENT "累计成交金额",
            net_amount DECIMAL COMMENT "净买入金额",
            sh_amount DECIMAL COMMENT "沪市成交金额",
            sh_net_amount DECIMAL COMMENT "沪市净买入金额",
            sh_buy DECIMAL COMMENT "沪市买入金额",
            sh_sell DECIMAL COMMENT "沪市卖出金额",
            sz_amount DECIMAL COMMENT "深市成交金额",
            sz_net_amount DECIMAL COMMENT "深市净买入金额",
            sz_buy DECIMAL COMMENT "深市买入金额",
            sz_sell DECIMAL COMMENT "深市卖出金额",           
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_沪深股票_行情数据_港股通十大成交股"

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
    market_type="",
    limit="",
    offset="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.ggt_top10(
            **{
                "ts_code": ts_code,
                "trade_date": trade_date,
                "start_date": start_date,
                "end_date": end_date,
                "market_type": market_type,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "trade_date",
                "ts_code",
                "name",
                "close",
                "p_change",
                "rank",
                "market_type",
                "amount",
                "net_amount",
                "sh_amount",
                "sh_net_amount",
                "sh_buy",
                "sh_sell",
                "sz_amount",
                "sz_net_amount",
                "sz_buy",
                "sz_sell",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_行情数据_港股通十大成交股"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_HangQingShuJu_GangGuTongShiDaChengJiaoGu"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
