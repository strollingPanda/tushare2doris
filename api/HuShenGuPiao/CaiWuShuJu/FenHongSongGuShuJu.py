# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=103


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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_FenHongSongGuShuJu"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            end_date VARCHAR(8) COMMENT "分送年度",
            ann_date VARCHAR(8) COMMENT "预案公告日（董事会）",
            div_proc VARCHAR(30) COMMENT "实施进度",
            stk_div DECIMAL COMMENT "每股送转",
            stk_bo_rate DECIMAL COMMENT "每股送股比例",
            stk_co_rate DECIMAL COMMENT "每股转增比例",
            cash_div DECIMAL COMMENT "每股分红（税后）",
            cash_div_tax DECIMAL COMMENT "每股分红（税前）",
            record_date VARCHAR(8) COMMENT "股权登记日",
            ex_date VARCHAR(8) COMMENT "除权除息日",
            pay_date VARCHAR(8) COMMENT "派息日",
            div_listdate VARCHAR(8) COMMENT "红股上市日",
            imp_ann_date VARCHAR(8) COMMENT "实施公告日",
            base_date VARCHAR(8) COMMENT "基准日",
            base_share DECIMAL COMMENT "实施基准股本（万）",
            update_flag VARCHAR(1) COMMENT "是否变更过（1表示变更）",
        )
        UNIQUE KEY(
                ts_code,
                end_date,
                ann_date,
                div_proc,
                stk_div,
                stk_bo_rate,
                stk_co_rate,
                cash_div,
                cash_div_tax,
                record_date,
                ex_date,
                pay_date,
                div_listdate,
                imp_ann_date,
                base_date,
                base_share,
                update_flag)
        COMMENT "tushare_沪深股票_财务数据_分红送股数据"

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
    ann_date="",
    end_date="",
    record_date="",
    ex_date="",
    imp_ann_date="",
    limit="",
    offset="",
):
    """执行具体下载操作"""
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.dividend(
            **{
                "ts_code": ts_code,
                "ann_date": ann_date,
                "end_date": end_date,
                "record_date": record_date,
                "ex_date": ex_date,
                "imp_ann_date": imp_ann_date,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "end_date",
                "ann_date",
                "div_proc",
                "stk_div",
                "stk_bo_rate",
                "stk_co_rate",
                "cash_div",
                "cash_div_tax",
                "record_date",
                "ex_date",
                "pay_date",
                "div_listdate",
                "imp_ann_date",
                "base_date",
                "base_share",
                "update_flag",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    print(df)
    return df, exception_status


def download():
    """下载 沪深股票_财务数据_分红送股数据"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_FenHongSongGuShuJu"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按公告日期下载
    basis.HuShunGuPiao_function.download_by_ann_date(table_name, download_execute, logger)


if __name__ == "__main__":
    create_table()
    download()
