# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=255


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
        + config["Ts_HuShenGuPiao_HangQingShuJu_BeiYongHangQing"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "股票代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            name VARCHAR(30) COMMENT "股票名称",
            pct_change DECIMAL COMMENT "涨跌幅",
            close DECIMAL COMMENT "收盘价",
            change DECIMAL COMMENT "涨跌额",
            open DECIMAL COMMENT "开盘价",
            high DECIMAL COMMENT "最高价",
            low DECIMAL COMMENT "最低价",
            pre_close DECIMAL COMMENT "昨收价",
            vol_ratio DECIMAL COMMENT "量比",
            turn_over DECIMAL COMMENT "换手率",
            swing DECIMAL COMMENT "振幅",
            vol DECIMAL COMMENT "成交量",
            amount DECIMAL COMMENT "成交额",
            selling DECIMAL COMMENT "内盘",
            buying DECIMAL COMMENT "外盘",
            total_share DECIMAL COMMENT "总股本(万)",
            float_share DECIMAL COMMENT "流通股本(万)",
            pe DECIMAL COMMENT "市盈(动)",
            industry VARCHAR(30) COMMENT "所属行业",
            area VARCHAR(30) COMMENT "所属地域",
            float_mv DECIMAL COMMENT "流通市值",
            total_mv DECIMAL COMMENT "总市值",
            avg_price DECIMAL COMMENT "平均价",
            strength DECIMAL COMMENT "强弱度(%)",
            activity DECIMAL COMMENT "活跃度(%)",
            avg_turnover DECIMAL COMMENT "笔换手",
            attack DECIMAL COMMENT "攻击波(%)",
            interval_3 DECIMAL COMMENT "近3月涨幅",
            interval_6 DECIMAL COMMENT "近6月涨幅",
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_沪深股票_行情数据_备用行情"

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
    offset="",
    limit="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.bak_daily(
            **{
                "ts_code": ts_code,
                "trade_date": trade_date,
                "start_date": start_date,
                "end_date": end_date,
                "offset": offset,
                "limit": limit,
            },
            fields=[
                "ts_code",
                "trade_date",
                "name",
                "pct_change",
                "close",
                "change",
                "open",
                "high",
                "low",
                "pre_close",
                "vol_ratio",
                "turn_over",
                "swing",
                "vol",
                "amount",
                "selling",
                "buying",
                "total_share",
                "float_share",
                "pe",
                "industry",
                "area",
                "float_mv",
                "total_mv",
                "avg_price",
                "strength",
                "activity",
                "avg_turnover",
                "attack",
                "interval_3",
                "interval_6",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_行情数据_备用行情"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_HangQingShuJu_BeiYongHangQing"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
