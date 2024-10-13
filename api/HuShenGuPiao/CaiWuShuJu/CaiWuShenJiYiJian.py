# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=80
# 默认的调取接口必须指定ts_code，在默认接口后加_vip则无需指定ts_code。
# *_vip接口需要5000积分才可调用。
# 注：tushare网页中并没有说这个指标可以用_vip，但其实这个指标只有用_vip接口才能下载指定ann_date的信息


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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_CaiWuShenJiYiJian"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS股票代码",
            ann_date VARCHAR(8) COMMENT "公告日期",
            end_date VARCHAR(8) COMMENT "报告期",
            audit_result STRING COMMENT "审计结果",
            audit_fees DECIMAL COMMENT "审计总费用（元）",
            audit_agency STRING COMMENT "会计事务所",
            audit_sign STRING COMMENT "签字会计师",
        )
        UNIQUE KEY(ts_code,ann_date,end_date)
        COMMENT "tushare_沪深股票_财务数据_财务审计意见"

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
    start_date="",
    end_date="",
    period="",
    limit="",
    offset="",
):
    """执行具体下载操作"""
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.fina_audit_vip(
            **{
                "ts_code": ts_code,
                "ann_date": ann_date,
                "start_date": start_date,
                "end_date": end_date,
                "period": period,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "ann_date",
                "end_date",
                "audit_result",
                "audit_fees",
                "audit_agency",
                "audit_sign",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_财务数据_财务审计意见"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_CaiWuShenJiYiJian"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # # 按公告日期下载
    # basis.basis_function.download_by_ann_date(table_name, download_execute, logger)

    # 按报告期下载
    basis.HuShunGuPiao_function.download_by_period(table_name, download_execute, logger)


if __name__ == "__main__":
    create_table()
    download()
