# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time
import basis.ZhaiQuan_function

# https://tushare.pro/document/2?doc_id=247


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiZhuanGuJieGuo"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_ZhaiQuan_KeZhuanZhaiZhuanGuJieGuo"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "债券代码",
            bond_short_name VARCHAR(50) COMMENT "债券简称",
            publish_date VARCHAR(50) COMMENT "公告日期",
            end_date VARCHAR(50) COMMENT "统计截止日期",
            issue_size DECIMAL COMMENT "可转债发行总额",
            convert_price_initial DECIMAL COMMENT "初始转换价格",
            convert_price DECIMAL COMMENT "本次转换价格",
            convert_val DECIMAL COMMENT "本次转股金额",
            convert_vol DECIMAL COMMENT "本次转股数量",
            convert_ratio DECIMAL COMMENT "本次转股比例",
            acc_convert_val DECIMAL COMMENT "累计转股金额",
            acc_convert_vol DECIMAL COMMENT "累计转股数量",
            acc_convert_ratio DECIMAL COMMENT "累计转股比例",
            remain_size DECIMAL COMMENT "可转债剩余金额",
            total_shares DECIMAL COMMENT "转股后总股本"
        )
        UNIQUE KEY(ts_code,bond_short_name,publish_date,end_date)
        COMMENT "tushare-债券_可转债转股结果"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, ts_code="", limit=2000, offset=""):
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
            df_local = pro.cb_share(
                **{"ts_code": ts_code, "limit": limit, "offset": offset},
                fields=[
                    "ts_code",
                    "bond_short_name",
                    "publish_date",
                    "end_date",
                    "issue_size",
                    "convert_price_initial",
                    "convert_price",
                    "convert_val",
                    "convert_vol",
                    "convert_ratio",
                    "acc_convert_val",
                    "acc_convert_vol",
                    "acc_convert_ratio",
                    "remain_size",
                    "total_shares",
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
    """下载 债券_可转债转股结果："""
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiZhuanGuJieGuo"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 所有可转债代码
    ts_code_all = basis.ZhaiQuan_function.get_ts_code_of_convertible_bond()

    # 下载数据
    for list_member in ts_code_all:
        logger.info("downloading " + table_name + ". ts_code: " + list_member)
        df = download_execute(pro, logger, ts_code=list_member)  # 下载数据
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
