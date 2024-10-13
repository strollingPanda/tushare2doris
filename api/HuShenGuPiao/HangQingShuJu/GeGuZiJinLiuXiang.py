# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=170


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
        + config["Ts_HuShenGuPiao_HangQingShuJu_GeGuZiJinLiuXiang"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            buy_sm_vol BIGINT COMMENT "小单买入量（手）",
            buy_sm_amount DECIMAL COMMENT "小单买入金额（万元）",
            sell_sm_vol BIGINT COMMENT "小单卖出量（手）",
            sell_sm_amount DECIMAL COMMENT "小单卖出金额（万元）",
            buy_md_vol BIGINT COMMENT "中单买入量（手）",
            buy_md_amount DECIMAL COMMENT "中单买入金额（万元）",
            sell_md_vol BIGINT COMMENT "中单卖出量（手）",
            sell_md_amount DECIMAL COMMENT "中单卖出金额（万元）",
            buy_lg_vol BIGINT COMMENT "大单买入量（手）",
            buy_lg_amount DECIMAL COMMENT "大单买入金额（万元）",
            sell_lg_vol BIGINT COMMENT "大单卖出量（手）",
            sell_lg_amount DECIMAL COMMENT "大单卖出金额（万元）",
            buy_elg_vol BIGINT COMMENT "特大单买入量（手）",
            buy_elg_amount DECIMAL COMMENT "特大单买入金额（万元）",
            sell_elg_vol BIGINT COMMENT "特大单卖出量（手）",
            sell_elg_amount DECIMAL COMMENT "特大单卖出金额（万元）",
            net_mf_vol BIGINT COMMENT "净流入量（手）",
            net_mf_amount DECIMAL COMMENT "净流入额（万元）",
            trade_count BIGINT COMMENT "交易笔数",            
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_沪深股票_行情数据_个股资金流向"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
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
        df = pro.moneyflow(
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
                "buy_sm_vol",
                "buy_sm_amount",
                "sell_sm_vol",
                "sell_sm_amount",
                "buy_md_vol",
                "buy_md_amount",
                "sell_md_vol",
                "sell_md_amount",
                "buy_lg_vol",
                "buy_lg_amount",
                "sell_lg_vol",
                "sell_lg_amount",
                "buy_elg_vol",
                "buy_elg_amount",
                "sell_elg_vol",
                "sell_elg_amount",
                "net_mf_vol",
                "net_mf_amount",
                "trade_count",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_行情数据_个股资金流向"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_HangQingShuJu_GeGuZiJinLiuXiang"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
