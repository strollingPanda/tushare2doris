# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=262


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
        + config["Ts_HuShenGuPiao_JiChuShuJu_BeiYongLieBiao"]["table_name"]
        + """
        (
            trade_date VARCHAR(50) COMMENT "交易日期",
            ts_code VARCHAR(50) COMMENT "TS股票代码",
            name VARCHAR(50) COMMENT "股票名称",
            industry VARCHAR(200) COMMENT "行业",
            area VARCHAR(50) COMMENT "地域",
            pe DECIMAL COMMENT "市盈率（动）",
            float_share DECIMAL COMMENT "流通股本（亿）",
            total_share DECIMAL COMMENT "总股本（亿）",
            total_assets DECIMAL COMMENT "总资产（亿）",
            liquid_assets DECIMAL COMMENT "流动资产（亿）",
            fixed_assets DECIMAL COMMENT "固定资产（亿）",
            reserved DECIMAL COMMENT "公积金",
            reserved_pershare DECIMAL COMMENT "每股公积金",
            eps DECIMAL COMMENT "每股收益",
            bvps DECIMAL COMMENT "每股净资产",
            pb DECIMAL COMMENT "市净率",
            list_date VARCHAR(8) COMMENT "上市日期",
            undp DECIMAL COMMENT "未分配利润",
            per_undp DECIMAL COMMENT "每股未分配利润",
            rev_yoy DECIMAL COMMENT "收入同比（%）",
            profit_yoy DECIMAL COMMENT "利润同比（%）",
            gpr DECIMAL COMMENT "毛利率（%）",
            npr DECIMAL COMMENT "净利润率（%）",
            holder_num BIGINT(50) COMMENT "股东人数"
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_沪深股票_基础数据_备用列表"

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
    offset="",
    limit="",
):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.bak_basic(
            **{"trade_date": trade_date, "ts_code": ts_code, "limit": limit, "offset": offset},
            fields=[
                "trade_date",
                "ts_code",
                "name",
                "industry",
                "area",
                "pe",
                "float_share",
                "total_share",
                "total_assets",
                "liquid_assets",
                "fixed_assets",
                "reserved",
                "reserved_pershare",
                "eps",
                "bvps",
                "pb",
                "list_date",
                "undp",
                "per_undp",
                "rev_yoy",
                "profit_yoy",
                "gpr",
                "npr",
                "holder_num",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_基础数据_备用列表"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_JiChuShuJu_BeiYongLieBiao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按日期下载
    basis.basis_function.download_by_date(table_name, download_execute, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
