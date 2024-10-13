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

# https://tushare.pro/document/2?doc_id=94


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
        + config["Ts_ZhiShu_ZhiShuJiBenXinXi"]["table_name"]
        + """
        (
            ts_code	VARCHAR(50)	COMMENT "TS代码",
            name VARCHAR(100) COMMENT "简称", 
            fullname VARCHAR(100) COMMENT "指数全称", 
            market VARCHAR(100) COMMENT "市场", 
            publisher VARCHAR(100) COMMENT "发布方", 
            index_type VARCHAR(100) COMMENT "指数风格", 
            category VARCHAR(100) COMMENT "指数类别", 
            base_date VARCHAR(8) COMMENT "基期", 
            base_point DECIMAL COMMENT "基点", 
            list_date VARCHAR(8) COMMENT "发布日期", 
            weight_rule	VARCHAR(100)COMMENT "加权方式", 
            desc_string STRING COMMENT "描述", 
            exp_date VARCHAR(8) COMMENT "终止日期"
        )
        UNIQUE KEY(ts_code,name)
        COMMENT "tushare_指数_指数基本信息"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro, logger, ts_code="", market="", publisher="", category="", name="", limit="", offset=""
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.index_basic(
            **{
                "ts_code": ts_code,
                "market": market,
                "publisher": publisher,
                "category": category,
                "name": name,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "name",
                "fullname",
                "market",
                "publisher",
                "index_type",
                "category",
                "base_date",
                "base_point",
                "list_date",
                "weight_rule",
                "desc",
                "exp_date",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 指数_指数基本信息"""

    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_ZhiShuJiBenXinXi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按市场下载，下载指定市场的所有指数基本信息
    market_all = {
        "MSCI": "MSCI指数",
        "CSI": "中证指数",
        "SSE": "上交所指数",
        "SZSE": "深交所指数",
        "CICC": "中金指数",
        "SW": "申万指数",
        "OTH": "其他指数",
    }
    # 下载每一个市场的信息
    for market in market_all.keys():
        logger.info("downloading " + table_name + ". market: " + market)
        df = download_execute(pro, logger, market=market)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
