# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=250


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
        + config["Ts_GangGu_GangGuJiaoYiRiLi"]["table_name"]
        + """
        (
            cal_date VARCHAR(8) COMMENT "日历日期",
            is_open BIGINT COMMENT "是否交易 '0'休市 '1'交易",
            pretrade_date VARCHAR(8) COMMENT "上一个交易日"
        )
        UNIQUE KEY(cal_date)
        COMMENT "tushare_港股_港股交易日历"

        DISTRIBUTED BY HASH(cal_date) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro,
    logger,
    start_date="",
    end_date="",
    is_open="",
    exchange="",
    limit=2000,
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
        # 若首次下载，或下载的数据大于等于limit，继续循环
        while num_times_download == 0 or df_local.shape[0] >= limit:
            df_local = pro.hk_tradecal(
                **{
                    "start_date": start_date,
                    "end_date": end_date,
                    "is_open": is_open,
                    "exchange": exchange,
                    "limit": limit,
                    "offset": offset,
                },
                fields=["cal_date", "is_open", "pretrade_date"]
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
    """下载 港股_港股交易日历"""

    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_GangGu_GangGuJiaoYiRiLi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    logger.info("downloading " + table_name)
    df = download_execute(pro, logger)  # 执行下载
    basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
    time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
