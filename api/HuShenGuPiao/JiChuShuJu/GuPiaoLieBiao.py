# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time


# https://tushare.pro/document/2?doc_id=25


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_JiChuShuJu_GuPiaoLieBiao"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_HuShenGuPiao_JiChuShuJu_GuPiaoLieBiao"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            symbol VARCHAR(50) COMMENT "股票代码",
            name VARCHAR(50) COMMENT "股票名称",
            area VARCHAR(50) COMMENT "地域",
            industry VARCHAR(200) COMMENT "所属行业",
            fullname VARCHAR(200) COMMENT "股票全称",
            enname VARCHAR(200) COMMENT "英文全称",
            cnspell VARCHAR(50) COMMENT "拼音缩写",
            market VARCHAR(50) COMMENT "市场类型",
            exchange VARCHAR(50) COMMENT "交易所代码",
            curr_type VARCHAR(50) COMMENT "交易货币",
            list_status VARCHAR(50) COMMENT "上市状态 L上市 D退市 P暂停上市",
            list_date VARCHAR(50) COMMENT "上市日期",
            delist_date VARCHAR(50) COMMENT "退市日期",
            is_hs VARCHAR(50) COMMENT "是否沪深港通标的，N否 H沪股通 S深股通",
            act_name VARCHAR(200) COMMENT "实控人名称",
            act_ent_type VARCHAR(50) COMMENT "实控人企业性质"
        )
        DUPLICATE KEY(ts_code)
        COMMENT "tushare_沪深股票_基础数据_股票列表"

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
    name="",
    exchange="",
    market="",
    is_hs="",
    list_status="",
    limit="",
    offset="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.stock_basic(
            **{
                "ts_code": ts_code,
                "name": name,
                "exchange": exchange,
                "market": market,
                "is_hs": is_hs,
                "list_status": list_status,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "symbol",
                "name",
                "area",
                "industry",
                "fullname",
                "enname",
                "cnspell",
                "market",
                "exchange",
                "curr_type",
                "list_status",
                "list_date",
                "delist_date",
                "is_hs",
                "act_name",
                "act_ent_type",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_基础数据_股票列表"""

    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_JiChuShuJu_GuPiaoLieBiao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 下载每一个交易所数据
    logger.info("downloading " + table_name)
    df = download_execute(pro, logger)  # 执行下载
    basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
    time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
