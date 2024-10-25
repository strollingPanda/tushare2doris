# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=158


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_QiQuan_QiQuanHeYueXinXi"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_QiQuan_QiQuanHeYueXinXi"]["table_name"]
        + """
    (
        ts_code VARCHAR(50) COMMENT "TS代码",
        exchange VARCHAR(50) COMMENT "交易市场",
        name VARCHAR(200) COMMENT "合约名称",
        per_unit DECIMAL COMMENT "合约单位",
        opt_code VARCHAR(50) COMMENT "标准合约代码",
        opt_type VARCHAR(50) COMMENT "合约类型",
        call_put VARCHAR(50) COMMENT "期权类型",
        exercise_type VARCHAR(50) COMMENT "行权方式",
        exercise_price DECIMAL COMMENT "行权价格",
        s_month VARCHAR(50) COMMENT "结算月",
        maturity_date VARCHAR(8) COMMENT "到期日",
        list_price DECIMAL COMMENT "挂牌基准价",
        list_date VARCHAR(8) COMMENT "开始交易日期",
        delist_date VARCHAR(8) COMMENT "最后交易日期",
        last_edate VARCHAR(8) COMMENT "最后行权日期",
        last_ddate VARCHAR(8) COMMENT "最后交割日期",
        quote_unit VARCHAR(50) COMMENT "报价单位",
        min_price_chg VARCHAR(50) COMMENT "最小价格波幅"
    )
    DUPLICATE KEY(ts_code)
    COMMENT "tushare-期权_期权合约信息"

    DISTRIBUTED BY HASH(ts_code) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, ts_code="", exchange="", opt_code="", call_put="", offset="", limit=8000):
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
            df_local = pro.opt_basic(
                **{
                    "ts_code": ts_code,
                    "exchange": exchange,
                    "opt_code": opt_code,
                    "call_put": call_put,
                    "offset": offset,
                    "limit": limit,
                },
                fields=[
                    "ts_code",
                    "exchange",
                    "name",
                    "per_unit",
                    "opt_code",
                    "opt_type",
                    "call_put",
                    "exercise_type",
                    "exercise_price",
                    "s_month",
                    "maturity_date",
                    "list_price",
                    "list_date",
                    "delist_date",
                    "last_edate",
                    "last_ddate",
                    "quote_unit",
                    "min_price_chg",
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
    """下载 期权_期权合约信息："""
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_QiQuan_QiQuanHeYueXinXi"]["table_name"]

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
