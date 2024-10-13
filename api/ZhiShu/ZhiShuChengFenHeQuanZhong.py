# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.ZhiShu_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=96


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
        + config["Ts_ZhiShu_ZhiShuChengFenHeQuanZhong"]["table_name"]
        + """
        (
            index_code	VARCHAR(50)	COMMENT "指数代码",
            con_code VARCHAR(50) COMMENT "成分代码",
            trade_date VARCHAR(8) COMMENT "交易日期",
            weight DECIMAL COMMENT "权重",
        )
        UNIQUE KEY(
                    index_code,
                    con_code,
                    trade_date)
        COMMENT "tushare_指数_指数成分和权重"

        DISTRIBUTED BY HASH(index_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro,
    logger,
    index_code="",
    trade_date="",
    start_date="",
    end_date="",
    ts_code="",
    limit=5000,
    offset="",
):
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    """执行具体下载操作"""
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pandas.DataFrame()  # 最终函数的返回值
        df_local = pandas.DataFrame()  # 暂时设为空值，以执行循环
        num_times_download = 0  # 已经下载了几次
        # 若首次下载，或下载的数据大于等于limit，继续循环
        while num_times_download == 0 or df_local.shape[0] >= limit:
            df_local = pro.index_weight(
                **{
                    "index_code": index_code,
                    "trade_date": trade_date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "ts_code": ts_code,
                    "limit": limit,
                    "offset": offset,
                },
                fields=["index_code", "con_code", "trade_date", "weight"]
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
    """下载 指数_指数成分和权重"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_ZhiShuChengFenHeQuanZhong"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 要下载的指数代码
    index_code_all = ["000001.SH", "000300.SH"]

    for index_code in index_code_all:
        basis.ZhiShu_function.download_index_weight(table_name, download_execute, index_code, logger)


if __name__ == "__main__":
    create_table()
    download()
