# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=187


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
        + config["Ts_ZhaiQuan_KeZhuanZhaiHangQing"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "转债代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            pre_close DECIMAL COMMENT "昨收盘价(元)",
            open DECIMAL COMMENT "开盘价(元)",
            high DECIMAL COMMENT "最高价(元)",
            low DECIMAL COMMENT "最低价(元)",
            close DECIMAL COMMENT "收盘价(元)",
            change DECIMAL COMMENT "涨跌(元)",
            pct_chg DECIMAL COMMENT "涨跌幅(%)",
            vol DECIMAL COMMENT "成交量(手)",
            amount DECIMAL COMMENT "成交金额(万元)",
            bond_value DECIMAL COMMENT "纯债价值",
            bond_over_rate DECIMAL COMMENT "纯债溢价率(%)",
            cb_value DECIMAL COMMENT "转股价值",
            cb_over_rate DECIMAL COMMENT "转股溢价率(%)"
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_债券_可转债行情"

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
    limit=2000,
    offset="",
):
    """
    执行具体下载操作
    """
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pandas.DataFrame()  # 最终函数的返回值
        df_local = pandas.DataFrame()  # 暂时设为空值，以执行循环
        num_times_download = 0  # 已经下载了几次
        # 若首次下载，或下载的数据大于等于limit，继续循环
        while num_times_download == 0 or df_local.shape[0] >= limit:
            df_local = pro.cb_daily(
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
                    "pre_close",
                    "open",
                    "high",
                    "low",
                    "close",
                    "change",
                    "pct_chg",
                    "vol",
                    "amount",
                    "bond_value",
                    "bond_over_rate",
                    "cb_value",
                    "cb_over_rate",
                ]
            )  # 从tushare下载
            num_times_download += 1  # 已经下载了几次
            df = pandas.concat([df, df_local])  # 将df_local加入结果
            offset = num_times_download * limit  # 计算新的offset
            time.sleep(config["regular_gap"])  # 等待一段时间再继续下载
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 债券_可转债行情"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiHangQing"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
