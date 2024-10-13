# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time
import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=335


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
        + config["Ts_ZhiShu_ShenWanHangYeChengFen"]["table_name"]
        + """
        (
            l1_code VARCHAR(50) COMMENT "L1代码",
            l1_name VARCHAR(50) COMMENT "L1名称",
            l2_code VARCHAR(50) COMMENT "L2代码",
            l2_name VARCHAR(50) COMMENT "L2名称",
            l3_code VARCHAR(50) COMMENT "L3代码",
            l3_name VARCHAR(50) COMMENT "L3名称",
            ts_code VARCHAR(50) COMMENT "成分股票代码",
            name VARCHAR(50) COMMENT "成分股票名称",
            in_date VARCHAR(8) COMMENT "纳入日期",
            out_date VARCHAR(8) COMMENT "剔除日期",
            is_new VARCHAR(1) COMMENT "是否最新Y是N否"
        )
        UNIQUE KEY(l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code)
        COMMENT "tushare_指数-申万行业成分"

        DISTRIBUTED BY HASH(l1_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(
    pro, logger, l1_code="", l2_code="", l3_code="", is_new="", ts_code="", limit=2000, offset=""
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
            df_local = pro.index_member_all(
                **{
                    "l1_code": l1_code,
                    "l2_code": l2_code,
                    "l3_code": l3_code,
                    "is_new": is_new,
                    "ts_code": ts_code,
                    "limit": limit,
                    "offset": offset,
                },
                fields=[
                    "l1_code",
                    "l1_name",
                    "l2_code",
                    "l2_name",
                    "l3_code",
                    "l3_name",
                    "ts_code",
                    "name",
                    "in_date",
                    "out_date",
                    "is_new",
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
    """下载 指数-申万行业成分"""

    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_ZhiShu_ShenWanHangYeChengFen"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    logger.info("downloading " + table_name)
    df = download_execute(pro, logger)  # 执行下载
    basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
    time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
