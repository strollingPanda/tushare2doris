# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=343


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
        + config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_HangYeZiJinLiuXiang_THS"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期",
            ts_code VARCHAR(50) COMMENT "板块代码",
            industry VARCHAR(50) COMMENT "板块名称",
            lead_stock VARCHAR(50) COMMENT "领涨股票名称",
            close DECIMAL COMMENT "收盘指数",
            pct_change DECIMAL COMMENT "指数涨跌幅",
            company_num BIGINT COMMENT "公司数量",
            pct_change_stock DECIMAL COMMENT "领涨股涨跌幅",
            close_price DECIMAL COMMENT "领涨股最新价",
            net_buy_amount DECIMAL COMMENT "流入资金(元)",
            net_sell_amount DECIMAL COMMENT "流出资金(元)",
            net_amount DECIMAL COMMENT "净额(元)"          
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_沪深股票_资金流向数据_行业资金流向（THS）"

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
    limit="",
    offset="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.moneyflow_ind_ths(
            **{
                "ts_code": ts_code,
                "trade_date": trade_date,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "trade_date",
                "ts_code",
                "industry",
                "lead_stock",
                "close",
                "pct_change",
                "company_num",
                "pct_change_stock",
                "close_price",
                "net_buy_amount",
                "net_sell_amount",
                "net_amount",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_资金流向数据_行业资金流向（THS）"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_HangYeZiJinLiuXiang_THS"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
