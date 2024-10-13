# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.ZhiShu_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=95


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
        + config["Ts_ZhiShu_ZhiShuRiXianHangQing"]["table_name"]
        + """
        (
            ts_code	VARCHAR(50)	COMMENT "TS指数代码",
            trade_date VARCHAR(8) COMMENT "交易日",
            close DECIMAL COMMENT "收盘点位V",
            open DECIMAL COMMENT "开盘点位",
            high DECIMAL COMMENT "最高点位",
            low DECIMAL COMMENT "最低点位",
            pre_close DECIMAL COMMENT "昨日收盘点",
            change DECIMAL COMMENT "涨跌点",
            pct_chg DECIMAL COMMENT "涨跌幅（%）",
            vol DECIMAL COMMENT "成交量（手）",
            amount DECIMAL COMMENT "成交额（千元）"
        )
        UNIQUE KEY(
                    ts_code,
                    trade_date)
        COMMENT "tushare_指数_指数日线行情"

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
    limit=5000,
    offset="",
):
    """执行具体下载操作"""
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pandas.DataFrame()  # 最终函数的返回值
        df_local = pandas.DataFrame()  # 暂时设为空值，以执行循环
        num_times_download = 0  # 已经下载了几次
        while (
            num_times_download == 0 or df_local.shape[0] >= limit
        ):  # 若首次下载，或下载的数据大于等于limit，继续循环
            df_local = pro.index_daily(
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
                    "open",
                    "high",
                    "low",
                    "pre_close",
                    "change",
                    "pct_chg",
                    "vol",
                    "amount",
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
    """下载 指数_指数日线行情"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_ZhiShuRiXianHangQing"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # # 在指定市场里获得所有指数的日线行情
    # market_all = {
    #     "CSI": "中证指数",
    #     "SSE": "上交所指数",
    #     "SZSE": "深交所指数",
    #     "CICC": "中金指数",
    #     "SW": "申万指数",
    #     "MSCI": "MSCI指数",
    #     "OTH": "其他指数",
    # }
    # # tushare中MSCI日线行情为空（20240928记录）
    # for market in market_all.keys():
    #     # 根据输入的市场，获取该市场全部指数代码
    #     ts_code_all = basis.ZhiShu_function.get_ts_code_of_index_by_market(market)
    #     for ts_code in ts_code_all:
    #         # 按指数代码下载日线行情
    #         basis.ZhiShu_function.download_index_daily_by_ts_code(table_name, download_execute, ts_code, logger)

    # 下载指定指数的基本信息
    ts_code_all = ["000001.SH", "000300.SH"]

    # 下载每一个指数的基本信息
    for ts_code in ts_code_all:
        basis.ZhiShu_function.download_index_daily_by_ts_code(table_name, download_execute, ts_code, logger)


if __name__ == "__main__":
    create_table()
    download()
