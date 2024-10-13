# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time
import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=191


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
        + config["Ts_GangGu_GangGuJiChuXinXi"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            name VARCHAR(50) COMMENT "股票简称",
            fullname VARCHAR(200) COMMENT "公司全称",
            enname VARCHAR(200) COMMENT "英文名称",
            cn_spell VARCHAR(200) COMMENT "拼音",
            market VARCHAR(50) COMMENT "市场类别",
            list_status VARCHAR(50) COMMENT "上市状态",
            list_date VARCHAR(8) COMMENT "上市日期",
            delist_date VARCHAR(8) COMMENT "退市日期",
            trade_unit DECIMAL COMMENT "交易单位",
            isin VARCHAR(50) COMMENT "ISIN代码",
            curr_type VARCHAR(50) COMMENT "货币代码"
        )
        UNIQUE KEY(ts_code,name)
        COMMENT "tushare_港股_港股基础信息"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, ts_code="", list_status="", limit="", offset=""):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.hk_basic(
            **{"ts_code": ts_code, "list_status": list_status, "limit": limit, "offset": offset},
            fields=[
                "ts_code",
                "name",
                "fullname",
                "enname",
                "cn_spell",
                "market",
                "list_status",
                "list_date",
                "delist_date",
                "trade_unit",
                "isin",
                "curr_type",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 港股_港股基础信息"""

    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_GangGu_GangGuJiChuXinXi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    logger.info("downloading " + table_name)
    df = download_execute(pro, logger)  # 执行下载
    basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
    time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
