# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris

# https://tushare.pro/document/2?doc_id=344


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
        + config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_BanKuaiZiJinLiuXiang_DC"]["table_name"]
        + """
        (
            trade_date VARCHAR(8) COMMENT "交易日期",
            ts_code VARCHAR(50) COMMENT "股票代码",
            name VARCHAR(50) COMMENT "股票名称",
            pct_change DECIMAL COMMENT "涨跌幅（%）",
            close DECIMAL COMMENT "最新价（元）",
            net_amount DECIMAL COMMENT "今日主力净流入 净额（元）",
            net_amount_rate DECIMAL COMMENT "今日主力净流入净占比%",
            buy_elg_amount DECIMAL COMMENT "今日超大单净流入 净额（元）",
            buy_elg_amount_rate DECIMAL COMMENT "今日超大单净流入 净占比%",
            buy_lg_amount DECIMAL COMMENT "今日大单净流入 净额（元）",
            buy_lg_amount_rate DECIMAL COMMENT "今日大单净流入 净占比%",
            buy_md_amount DECIMAL COMMENT "今日中单净流入 净额（元）",
            buy_md_amount_rate DECIMAL COMMENT "今日中单净流入 净占比%",
            buy_sm_amount DECIMAL COMMENT "今日小单净流入 净额（元）",
            buy_sm_amount_rate DECIMAL COMMENT "今日小单净流入 净占比%",
            buy_sm_amount_stock VARCHAR(50) COMMENT "今日主力净流入最大股",
            rank BIGINT COMMENT "序号"      
        )
        UNIQUE KEY(trade_date,ts_code)
        COMMENT "tushare_沪深股票_资金流向数据_板块资金流向（DC）"

        DISTRIBUTED BY HASH(trade_date) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


# 从tushare下载
def download_from_tushare(
    pro,
    ts_code="",
    trade_date="",
    start_date="",
    end_date="",
    limit="",
    offset="",
):
    df_local = pro.moneyflow_ind_dc(
        **{
            "ts_code": ts_code,
            "trade_date": trade_date,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "offset": offset,
        },
        fields=[
            "trade_date",
            "ts_code",
            "name",
            "pct_change",
            "close",
            "net_amount",
            "net_amount_rate",
            "buy_elg_amount",
            "buy_elg_amount_rate",
            "buy_lg_amount",
            "buy_lg_amount_rate",
            "buy_md_amount",
            "buy_md_amount_rate",
            "buy_sm_amount",
            "buy_sm_amount_rate",
            "buy_sm_amount_stock",
            "rank",
        ]
    )
    return df_local


def download():
    """下载 沪深股票_资金流向数据_板块资金流向（DC）"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_ZiJinLiuXiangShuJu_BanKuaiZiJinLiuXiang_DC"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按开始、结束日期下载
    basis.basis_function.download_by_start_end_date(table_name, download_from_tushare, "沪深股票", logger)


if __name__ == "__main__":
    create_table()
    download()
