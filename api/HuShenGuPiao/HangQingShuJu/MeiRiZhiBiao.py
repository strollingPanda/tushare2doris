# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=32


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
        + config["Ts_HuShenGuPiao_HangQingShuJu_MeiRiZhiBiao"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS股票代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            close DECIMAL COMMENT "当日收盘价",
            turnover_rate DECIMAL COMMENT "换手率",
            turnover_rate_f DECIMAL COMMENT "换手率(自由流通股)",
            volume_ratio DECIMAL COMMENT "量比",
            pe DECIMAL COMMENT "市盈率（总市值/净利润）",
            pe_ttm DECIMAL COMMENT "市盈率（TTM）",
            pb DECIMAL COMMENT "市净率（总市值/净资产）",
            ps DECIMAL COMMENT "市销率",
            ps_ttm DECIMAL COMMENT "市销率（TTM）",
            dv_ratio DECIMAL COMMENT "股息率（%）",
            dv_ttm DECIMAL COMMENT "股息率（TTM） （%）",
            total_share DECIMAL COMMENT "总股本",
            float_share DECIMAL COMMENT "流通股本",
            free_share DECIMAL COMMENT "自由流通股本",
            total_mv DECIMAL COMMENT "总市值",
            circ_mv DECIMAL COMMENT "流通市值",
            limit_status BIGINT COMMENT "涨跌停状态",            
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_沪深股票_行情数据_每日指标"

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
        df = pro.daily_basic(
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
                "close",
                "turnover_rate",
                "turnover_rate_f",
                "volume_ratio",
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                "dv_ratio",
                "dv_ttm",
                "total_share",
                "float_share",
                "free_share",
                "total_mv",
                "circ_mv",
                "limit_status",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_行情数据_每日指标"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_HangQingShuJu_MeiRiZhiBiao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
