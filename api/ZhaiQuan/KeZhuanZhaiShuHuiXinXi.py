# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=269


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiShuHuiXinXi"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_ZhaiQuan_KeZhuanZhaiShuHuiXinXi"]["table_name"]
        + """
    (	
        ts_code VARCHAR(50) COMMENT "转债代码",
        call_type VARCHAR(50) COMMENT "赎回类型：到赎、强赎",
        is_call VARCHAR(50) COMMENT "是否赎回：公告到期赎回、公告强赎、公告不强赎",
        ann_date VARCHAR(8) COMMENT "公告日期",
        call_date VARCHAR(8) COMMENT "赎回日期",
        call_price DECIMAL COMMENT "赎回价格(含税，元/张)",
        call_price_tax DECIMAL COMMENT "赎回价格(扣税，元/张)",
        call_vol DECIMAL COMMENT "赎回债券数量(张)",
        call_amount DECIMAL COMMENT "赎回金额(万元)",
        payment_date VARCHAR(8) COMMENT "行权后款项到账日",
        call_reg_date VARCHAR(8) COMMENT "赎回登记日"
    )
    DUPLICATE KEY(ts_code,call_type)
    COMMENT "tushare-债券_可转债赎回信息"

    DISTRIBUTED BY HASH(ts_code) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, ts_code="", limit=1000, offset=""):
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
            df_local = pro.cb_call(
                **{"ts_code": ts_code, "limit": limit, "offset": offset},
                fields=[
                    "ts_code",
                    "call_type",
                    "is_call",
                    "ann_date",
                    "call_date",
                    "call_price",
                    "call_price_tax",
                    "call_vol",
                    "call_amount",
                    "payment_date",
                    "call_reg_date",
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
    """下载 债券_可转债赎回信息："""
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiShuHuiXinXi"]["table_name"]

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
