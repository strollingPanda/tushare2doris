# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=45
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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_YeJiYuGao"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS股票代码",
            ann_date VARCHAR(8) COMMENT "公告日期",
            end_date VARCHAR(8) COMMENT "报告期",
            type VARCHAR(20) COMMENT "业绩预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)",
            p_change_min DECIMAL COMMENT "预告净利润变动幅度下限（%）",
            p_change_max DECIMAL COMMENT "预告净利润变动幅度上限（%）",
            net_profit_min DECIMAL COMMENT "预告净利润下限（万元）",
            net_profit_max DECIMAL COMMENT "预告净利润上限（万元）",
            last_parent_net DECIMAL COMMENT "上年同期归属母公司净利润",
            notice_times BIGINT COMMENT "公布次数",
            first_ann_date VARCHAR(8) COMMENT "首次公告日",
            summary STRING COMMENT "业绩预告摘要",
            change_reason STRING COMMENT "业绩变动原因",
            update_flag VARCHAR(1) COMMENT "更新标志",
        )
        UNIQUE KEY(
                ts_code,
                ann_date,
                end_date,
                type,
                p_change_min,
                p_change_max,
                net_profit_min,
                net_profit_max,
                last_parent_net,
                notice_times,
                first_ann_date)
        COMMENT "tushare_沪深股票_财务数据_业绩预告"

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
    type="",
    limit="",
    offset="",
):
    """执行具体下载操作"""
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.forecast_vip(
            **{
                "ts_code": ts_code,
                "ann_date": ann_date,
                "start_date": start_date,
                "end_date": end_date,
                "period": period,
                "type": type,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "ann_date",
                "end_date",
                "type",
                "p_change_min",
                "p_change_max",
                "net_profit_min",
                "net_profit_max",
                "last_parent_net",
                "notice_times",
                "first_ann_date",
                "summary",
                "change_reason",
                "update_flag",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_财务数据_业绩预告"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_YeJiYuGao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # # 按公告日期下载
    # basis.basis_function.download_by_ann_date(table_name, download_execute, logger)

    # 按报告期下载
    basis.HuShunGuPiao_function.download_by_period(table_name, download_execute, logger)


if __name__ == "__main__":
    create_table()
    download()
