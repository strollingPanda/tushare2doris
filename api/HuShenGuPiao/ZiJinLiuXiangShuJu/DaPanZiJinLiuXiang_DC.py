# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=345


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
        + config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_DaPanZiJinLiuXiang_DC"]["table_name"]
        + """
        (
            trade_date VARCHAR(50) COMMENT "交易日期",
            close_sh DECIMAL COMMENT "上证收盘价（元）",
            ptc_change_sh DECIMAL COMMENT "上证涨跌幅(%)",
            close_sz DECIMAL COMMENT "深证收盘价（元）",
            pct_change_sz DECIMAL COMMENT "深证涨跌幅(%)",
            net_amount DECIMAL COMMENT "今日主力净流入 净额（元）",
            net_amount_rate DECIMAL COMMENT "今日主力净流入净占比%",
            buy_elg_amount DECIMAL COMMENT "今日超大单净流入 净额（元）",
            buy_elg_amount_rate DECIMAL COMMENT "今日超大单净流入 净占比%",
            buy_lg_amount DECIMAL COMMENT "今日大单净流入 净额（元）",
            buy_lg_amount_rate DECIMAL COMMENT "今日大单净流入 净占比%",
            buy_md_amount DECIMAL COMMENT "今日中单净流入 净额（元）",
            buy_md_amount_rate DECIMAL COMMENT "今日中单净流入 净占比%",
            buy_sm_amount DECIMAL COMMENT "今日小单净流入 净额（元）",
            buy_sm_amount_rate DECIMAL COMMENT "今日小单净流入 净占比%"          
        )
        UNIQUE KEY(trade_date)
        COMMENT "tushare_沪深股票_资金流向数据_大盘资金流向（DC）"

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
        df = pro.moneyflow_mkt_dc(
            **{
                "trade_date": trade_date,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "trade_date",
                "close_sh",
                "ptc_change_sh",
                "close_sz",
                "pct_change_sz",
                "net_amount",
                "net_amount_rate",
                "buy_elg_amount",
                "buy_elg_amount_rate",
                "buy_lg_amount",
                "buy_lg_amount_rate",
                "buy_md_amount",
                "buy_md_amount_rate",
                "buy_sm_amount",
                "buy_sm_amount_rate",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_资金流向数据_大盘资金流向（DC）"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_DaPanZiJinLiuXiang_DC"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
