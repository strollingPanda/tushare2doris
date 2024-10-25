# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=334


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
        + config["Ts_HuShenGuPiao_LiangRongJiZhuanRongTong_ZuoShiJieQuanJiaoYiHuiZong"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期（YYYYMMDD）",
            ts_code VARCHAR(50) COMMENT "股票代码",
            name VARCHAR(50) COMMENT "股票名称",
            ope_inv BIGINT COMMENT "期初余量(万股)",
            lent_qnt BIGINT COMMENT "融出数量(万股)",
            cls_inv BIGINT COMMENT "期末余量(万股)",
            end_bal BIGINT COMMENT "期末余额(万元)"        
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_沪深股票_两融及转融通_做市借券交易汇总"

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
    limit="",
    offset="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.slb_len_mm(
            **{
                "trade_date": trade_date,
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "offset": offset,
            },
            fields=["trade_date", "ts_code", "name", "ope_inv", "lent_qnt", "cls_inv", "end_bal"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_两融及转融通_做市借券交易汇总"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_LiangRongJiZhuanRongTong_ZuoShiJieQuanJiaoYiHuiZong"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
