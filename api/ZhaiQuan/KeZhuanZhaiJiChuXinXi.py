# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=185


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiJiChuXinXi"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_ZhaiQuan_KeZhuanZhaiJiChuXinXi"]["table_name"]
        + """
    (
        ts_code VARCHAR(50) COMMENT "转债代码",
        bond_full_name VARCHAR(200) COMMENT "转债名称",
        bond_short_name VARCHAR(50) COMMENT "转债简称",
        cb_code VARCHAR(50) COMMENT "转股申报代码",
        stk_code VARCHAR(50) COMMENT "正股代码",
        stk_short_name VARCHAR(50) COMMENT "正股简称",
        maturity DECIMAL COMMENT "发行期限（年）",
        par DECIMAL COMMENT "面值",
        issue_price DECIMAL COMMENT "发行价格",
        issue_size DECIMAL COMMENT "发行总额（元）",
        remain_size DECIMAL COMMENT "债券余额（元）",
        value_date VARCHAR(50) COMMENT "起息日期",
        maturity_date VARCHAR(50) COMMENT "到期日期",
        rate_type VARCHAR(50) COMMENT "利率类型",
        coupon_rate DECIMAL COMMENT "票面利率（%）",
        add_rate DECIMAL COMMENT "补偿利率（%）",
        pay_per_year BIGINT COMMENT "年付息次数",
        list_date VARCHAR(50) COMMENT "上市日期",
        delist_date VARCHAR(50) COMMENT "摘牌日",
        exchange VARCHAR(50) COMMENT "上市地点",
        conv_start_date VARCHAR(50) COMMENT "转股起始日",
        conv_end_date VARCHAR(50) COMMENT "转股截止日",
        conv_stop_date VARCHAR(50) COMMENT "停止转股日(提前到期)",
        first_conv_price DECIMAL COMMENT "初始转股价",
        conv_price DECIMAL COMMENT "最新转股价",
        rate_clause STRING COMMENT "利率说明",
        put_clause STRING COMMENT "赎回条款",
        maturity_put_price DECIMAL COMMENT "到期赎回价格(含税)",
        call_clause STRING COMMENT "回售条款",
        reset_clause STRING COMMENT "特别向下修正条款",
        conv_clause STRING COMMENT "转股条款",
        guarantor STRING COMMENT "担保人",
        guarantee_type VARCHAR(200) COMMENT "担保方式",
        issue_rating VARCHAR(50) COMMENT "发行信用等级",
        newest_rating VARCHAR(50) COMMENT "最新信用等级",
        rating_comp VARCHAR(200) COMMENT "最新评级机构"
    )
    DUPLICATE KEY(ts_code)
    COMMENT "tushare-债券_可转债基础信息"

    DISTRIBUTED BY HASH(ts_code) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, ts_code="", list_date="", exchange="", limit=2000, offset=""):
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
            df_local = pro.cb_basic(
                **{
                    "ts_code": ts_code,
                    "list_date": list_date,
                    "exchange": exchange,
                    "limit": limit,
                    "offset": offset,
                },
                fields=[
                    "ts_code",
                    "bond_full_name",
                    "bond_short_name",
                    "cb_code",
                    "stk_code",
                    "stk_short_name",
                    "maturity",
                    "par",
                    "issue_price",
                    "issue_size",
                    "remain_size",
                    "value_date",
                    "maturity_date",
                    "rate_type",
                    "coupon_rate",
                    "add_rate",
                    "pay_per_year",
                    "list_date",
                    "delist_date",
                    "exchange",
                    "conv_start_date",
                    "conv_end_date",
                    "conv_stop_date",
                    "first_conv_price",
                    "conv_price",
                    "rate_clause",
                    "put_clause",
                    "maturity_put_price",
                    "call_clause",
                    "reset_clause",
                    "conv_clause",
                    "guarantor",
                    "guarantee_type",
                    "issue_rating",
                    "newest_rating",
                    "rating_comp",
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
    """下载 债券_可转债基础信息："""
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiJiChuXinXi"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 下载数据
    logger.info("downloading " + table_name)
    df = download_execute(pro, logger)  # 下载数据
    basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
    time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
