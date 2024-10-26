# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=59


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
        + config["Ts_HuShenGuPiao_LiangRongJiZhuanRongTong_RongZiRongQuanJiaoYiMingXi"]["table_name"]
        + """
        (
            trade_date VARCHAR(50) COMMENT "交易日期",
            ts_code VARCHAR(50) COMMENT "TS股票代码",
            name VARCHAR(50) COMMENT "股票名称",
            rzye DECIMAL COMMENT "融资余额(元)",
            rqye DECIMAL COMMENT "融券余额(元)",
            rzmre DECIMAL COMMENT "融资买入额(元)",
            rqyl DECIMAL COMMENT "融券余量（手）",
            rzche DECIMAL COMMENT "融资偿还额(元)",
            rqchl DECIMAL COMMENT "融券偿还量(手)",
            rqmcl DECIMAL COMMENT "融券卖出量(股,份,手)",
            rzrqye DECIMAL COMMENT "融资融券余额(元)",
            update_time	DATETIME COMMENT "",
            create_time	DATETIME COMMENT "",           
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_沪深股票_两融及转融通_融资融券交易明细"

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
    ts_code="",
    start_date="",
    end_date="",
    limit=4000,
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
            df_local = pro.margin_detail(
                **{
                    "trade_date": trade_date,
                    "ts_code": ts_code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "limit": limit,
                    "offset": offset,
                },
                fields=[
                    "trade_date",
                    "ts_code",
                    "rzye",
                    "rqye",
                    "rzmre",
                    "rqyl",
                    "rzche",
                    "rqchl",
                    "rqmcl",
                    "rzrqye",
                    "name",
                    "update_time",
                    "create_time",
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
    """下载 沪深股票_两融及转融通_融资融券交易明细"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_LiangRongJiZhuanRongTong_RongZiRongQuanJiaoYiMingXi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
