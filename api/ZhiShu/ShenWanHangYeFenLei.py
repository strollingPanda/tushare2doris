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

# https://tushare.pro/document/2?doc_id=181


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_ShenWanHangYeFenLei"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_ZhiShu_ShenWanHangYeFenLei"]["table_name"]
        + """
        (
            index_code	VARCHAR(50)	COMMENT "指数代码",
            industry_name	VARCHAR(50)	COMMENT "行业名称",
            level	VARCHAR(50)	COMMENT "行业名称",
            industry_code	VARCHAR(50)	COMMENT "行业代码",
            is_pub	VARCHAR(1)	COMMENT "是否发布指数",
            parent_code	VARCHAR(50)	COMMENT "父级代码",
            src	VARCHAR(50)	COMMENT "行业分类（SW申万）"
        )
        DUPLICATE KEY(index_code)
        COMMENT "tushare_指数-申万行业分类"

        DISTRIBUTED BY HASH(index_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, index_code="", level="", src="", parent_code="", limit="", offset=""):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.index_classify(
            **{
                "index_code": index_code,
                "level": level,
                "src": src,
                "parent_code": parent_code,
                "limit": limit,
                "offset": offset,
            },
            fields=["index_code", "industry_name", "level", "industry_code", "is_pub", "parent_code", "src"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 指数-申万行业分类"""

    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_ShenWanHangYeFenLei"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 下载每一个市场的信息
    src_all = ["SW2014", "SW2021"]
    for src in src_all:
        logger.info("downloading " + table_name + ". src: " + src)
        df = download_execute(pro, logger, src=src)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
