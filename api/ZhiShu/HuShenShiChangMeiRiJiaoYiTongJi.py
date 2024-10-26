# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=215


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
        + config["Ts_ZhiShu_HuShenShiChangMeiRiJiaoYiTongJi"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期",
            ts_code VARCHAR(50) COMMENT "市场代码",
            ts_name VARCHAR(50) COMMENT "市场名称",
            com_count BIGINT COMMENT "挂牌数",
            total_share DECIMAL COMMENT "总股本（亿股）",
            float_share DECIMAL COMMENT "流通股本（亿股）",
            total_mv DECIMAL COMMENT "总市值（亿元）",
            float_mv DECIMAL COMMENT "流通市值（亿元）",
            amount DECIMAL COMMENT "交易金额（亿元）",
            vol DECIMAL COMMENT "成交量（亿股）",
            trans_count BIGINT COMMENT "成交笔数（万笔）",
            pe DECIMAL COMMENT "平均市盈率",
            tr DECIMAL COMMENT "换手率（％）",
            exchange VARCHAR(50) COMMENT "交易所"         
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_指数_沪深市场每日交易统计"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


# 从tushare下载
def download_from_tushare(
    pro,
    trade_date="",
    ts_code="",
    exchange="",
    start_date="",
    end_date="",
    limit="",
    offset="",
):
    df_local = pro.daily_info(
        **{
            "trade_date": trade_date,
            "ts_code": ts_code,
            "exchange": exchange,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "offset": offset,
        },
        fields=[
            "trade_date",
            "ts_code",
            "ts_name",
            "com_count",
            "total_share",
            "float_share",
            "total_mv",
            "float_mv",
            "amount",
            "vol",
            "trans_count",
            "pe",
            "tr",
            "exchange",
        ]
    )
    return df_local


def download():
    """下载 指数_沪深市场每日交易统计"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_HuShenShiChangMeiRiJiaoYiTongJi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按开始、结束日期下载
    basis.basis_function.download_by_start_end_date(table_name, download_from_tushare, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
