# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas
import time

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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_ZhuYingYeWuGouCheng"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            end_date VARCHAR(8) COMMENT "报告期",
            bz_item VARCHAR(300) COMMENT "主营业务项目",
            bz_code VARCHAR(4) COMMENT "项目代码",
            bz_sales DECIMAL COMMENT "主营业务收入(元)",
            bz_profit DECIMAL COMMENT "主营业务利润(元)",
            bz_cost DECIMAL COMMENT "主营业务成本(元)",
            curr_type VARCHAR(3) COMMENT "货币代码",
            update_flag VARCHAR(1) COMMENT "是否更新",
        )
        UNIQUE KEY(
                    ts_code,
                    end_date,
                    bz_item,
                    bz_code,
                    bz_sales,
                    bz_profit,
                    bz_cost,
                    curr_type,
                    update_flag)
        COMMENT "tushare_沪深股票_财务数据_主营业务构成"

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
    period="",
    type="",
    start_date="",
    end_date="",
    is_publish="",
    limit=5000,
    offset="",
):
    """执行具体下载操作"""
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pandas.DataFrame()  # 最终函数的返回值
        df_local = pandas.DataFrame()  # 暂时设为空值，以执行循环
        num_times_download = 0  # 已经下载了几次
        # 若首次下载，或下载的数据大于等于limit，继续循环
        while num_times_download == 0 or df_local.shape[0] >= limit:
            df_local = pro.fina_mainbz_vip(
                **{
                    "ts_code": ts_code,
                    "period": period,
                    "type": type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "is_publish": is_publish,
                    "limit": limit,
                    "offset": offset,
                },
                fields=[
                    "ts_code",
                    "end_date",
                    "bz_item",
                    "bz_code",
                    "bz_sales",
                    "bz_profit",
                    "bz_cost",
                    "curr_type",
                    "update_flag",
                ]
            )  # 从tushare下载
            num_times_download += 1  # 已经下载了几次
            df = pandas.concat([df, df_local])  # 将df_local加入结果
            offset = num_times_download * limit  # 计算新的offset
            time.sleep(config["regular_gap"])  # 等待一段时间再继续下载

    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_财务数据_主营业务构成"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_ZhuYingYeWuGouCheng"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按报告期下载
    basis.HuShunGuPiao_function.download_by_period(table_name, download_execute, logger)


if __name__ == "__main__":
    create_table()
    download()
