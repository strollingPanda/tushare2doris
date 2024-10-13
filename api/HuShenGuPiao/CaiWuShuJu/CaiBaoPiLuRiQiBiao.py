# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=81
# 默认的调取接口必须指定ts_code，在默认接口后加_vip则无需指定ts_code。
# *_vip接口需要5000积分才可调用。具体见上面链接。


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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_CaiBaoPiLuRiQiBiao"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            ann_date VARCHAR(8) COMMENT "最新披露公告日",
            end_date VARCHAR(8) COMMENT "报告期",
            pre_date VARCHAR(8) COMMENT "预计披露日期",
            actual_date VARCHAR(8) COMMENT "实际披露日期",
            modify_date VARCHAR(300) COMMENT "披露日期修正记录",            
        )
        UNIQUE KEY(
                    ts_code,
                    ann_date,
                    end_date,
                    pre_date,
                    actual_date,
                    modify_date
                    )
        COMMENT "tushare_沪深股票_财务数据_财报披露日期表"

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
    end_date="",
    pre_date="",
    actual_date="",
    limit="",
    offset="",
):
    """执行具体下载操作"""
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.disclosure_date(
            **{
                "ts_code": ts_code,
                "end_date": end_date,
                "pre_date": pre_date,
                "actual_date": actual_date,
                "offset": offset,
                "limit": limit,
            },
            fields=["ts_code", "ann_date", "end_date", "pre_date", "actual_date", "modify_date"]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_财务数据_财报披露日期表"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_CaiBaoPiLuRiQiBiao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按财报周期下载
    basis.HuShunGuPiao_function.download_by_end_date(table_name, download_execute, logger)


if __name__ == "__main__":
    create_table()
    download()
