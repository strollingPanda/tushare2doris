# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=259


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
        + config["Ts_ZhiShu_TongHuaShunGaiNianHeHangYeLieBiao"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "代码",
            name VARCHAR(50) COMMENT "名称",
            count BIGINT COMMENT "成分个数",
            exchange VARCHAR(50) COMMENT "交易所",
            list_date VARCHAR(8) COMMENT "上市日期",
            type VARCHAR(50) COMMENT "N概念指数S特色指数"     
        )
        UNIQUE KEY(ts_code)
        COMMENT "tushare_指数_同花顺概念和行业列表"

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
    exchange="",
    type="",
    limit="",
    offset="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.ths_index(
            **{"ts_code": ts_code, "exchange": exchange, "type": type, "limit": limit, "offset": offset},
            fields=["ts_code", "name", "count", "exchange", "list_date", "type"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 指数_同花顺概念和行业列表"""

    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_TongHuaShunGaiNianHeHangYeLieBiao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    logger.info("downloading " + table_name)
    df = download_execute(pro, logger)  # 执行下载
    basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris


if __name__ == "__main__":
    create_table()
    download()
