# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time


# https://tushare.pro/document/2?doc_id=104


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_JiChuShuJu_HuShenGuTongChengFenGu"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_HuShenGuPiao_JiChuShuJu_HuShenGuTongChengFenGu"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            hs_type VARCHAR(50) COMMENT "沪深港通类型SH沪SZ深",
            in_date VARCHAR(8) COMMENT "纳入日期",
            out_date VARCHAR(8) COMMENT "剔除日期",
            is_new VARCHAR(50) COMMENT "是否最新"
        )
        DUPLICATE KEY(ts_code)
        COMMENT "tushare_沪深股票_基础数据_沪深股通成份股"

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
    hs_type="",
    is_new="",
    ts_code="",
    limit="",
    offset="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.hs_const(
            **{"hs_type": hs_type, "is_new": is_new, "ts_code": ts_code, "limit": limit, "offset": offset},
            fields=["ts_code", "hs_type", "in_date", "out_date", "is_new"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_基础数据_沪深股通成份股"""
    hs_type_all = {
        "SH": "沪股通",
        "SZ": "深股通",
    }
    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_JiChuShuJu_HuShenGuTongChengFenGu"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 下载每一个交易所数据
    for hs_type in hs_type_all.keys():
        logger.info("downloading " + table_name + ". hs_type: " + hs_type)
        df = download_execute(pro, logger, hs_type=hs_type)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
