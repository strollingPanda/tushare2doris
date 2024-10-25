# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=186


# 创建表格
def create_table():
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取basis/config.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiFaXing"]["table_name"]
    # 删除原有表格
    basis.basis_function.drop_table(table_name)
    # 创建表格
    operation = (
        "CREATE TABLE IF NOT EXISTS "
        + config["database_name"]
        + "."
        + config["Ts_ZhaiQuan_KeZhuanZhaiFaXing"]["table_name"]
        + """
    (
        ts_code VARCHAR(50) COMMENT "转债代码",
        ann_date VARCHAR(8) COMMENT "发行公告日",
        res_ann_date VARCHAR(8) COMMENT "发行结果公告日",
        plan_issue_size DECIMAL COMMENT "计划发行总额（元）",
        issue_size DECIMAL COMMENT "发行总额（元）",
        issue_price DECIMAL COMMENT "发行价格",
        issue_type VARCHAR(50) COMMENT "发行方式",
        issue_cost DECIMAL COMMENT "发行费用（元）",
        onl_code VARCHAR(50) COMMENT "网上申购代码",
        onl_name VARCHAR(50) COMMENT "网上申购简称",
        onl_date VARCHAR(8) COMMENT "网上发行日期",
        onl_size DECIMAL COMMENT "网上发行总额（元）",
        onl_pch_vol DECIMAL COMMENT "网上发行有效申购数量（张）",
        onl_pch_num BIGINT COMMENT "网上发行有效申购户数",
        onl_pch_excess DECIMAL COMMENT "网上发行超额认购倍数",
        onl_winning_rate DECIMAL COMMENT "网上发行中签率（%）",
        shd_ration_code VARCHAR(50) COMMENT "老股东配售代码",
        shd_ration_name VARCHAR(50) COMMENT "老股东配售简称",
        shd_ration_date VARCHAR(8) COMMENT "老股东配售日",
        shd_ration_record_date VARCHAR(8) COMMENT "老股东配售股权登记日",
        shd_ration_pay_date VARCHAR(8) COMMENT "老股东配售缴款日",
        shd_ration_price DECIMAL COMMENT "老股东配售价格",
        shd_ration_ratio DECIMAL COMMENT "老股东配售比例",
        shd_ration_size DECIMAL COMMENT "老股东配售数量（张）",
        shd_ration_vol DECIMAL COMMENT "老股东配售有效申购数量（张）",
        shd_ration_num BIGINT COMMENT "老股东配售有效申购户数",
        shd_ration_excess DECIMAL COMMENT "老股东配售超额认购倍数",
        offl_size DECIMAL COMMENT "网下发行总额（元）",
        offl_deposit DECIMAL COMMENT "网下发行定金比例（%）",
        offl_pch_vol DECIMAL COMMENT "网下发行有效申购数量（张）",
        offl_pch_num BIGINT COMMENT "网下发行有效申购户数",
        offl_pch_excess DECIMAL COMMENT "网下发行超额认购倍数",
        offl_winning_rate DECIMAL COMMENT "网下发行中签率",
        lead_underwriter VARCHAR(2000) COMMENT "主承销商",
        lead_underwriter_vol DECIMAL COMMENT "主承销商包销数量（张）",
    )
    DUPLICATE KEY(ts_code,ann_date)
    COMMENT "tushare-债券_可转债发行"

    DISTRIBUTED BY HASH(ts_code) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, ts_code="", ann_date="", start_date="", end_date="", limit=2000, offset=""):
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
            df_local = pro.cb_issue(
                **{
                    "ts_code": ts_code,
                    "ann_date": ann_date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "limit": limit,
                    "offset": offset,
                },
                fields=[
                    "ts_code",
                    "ann_date",
                    "res_ann_date",
                    "plan_issue_size",
                    "issue_size",
                    "issue_price",
                    "issue_type",
                    "issue_cost",
                    "onl_code",
                    "onl_name",
                    "onl_date",
                    "onl_size",
                    "onl_pch_vol",
                    "onl_pch_num",
                    "onl_pch_excess",
                    "onl_winning_rate",
                    "shd_ration_code",
                    "shd_ration_name",
                    "shd_ration_date",
                    "shd_ration_record_date",
                    "shd_ration_pay_date",
                    "shd_ration_price",
                    "shd_ration_ratio",
                    "shd_ration_size",
                    "shd_ration_vol",
                    "shd_ration_num",
                    "shd_ration_excess",
                    "offl_size",
                    "offl_deposit",
                    "offl_pch_vol",
                    "offl_pch_num",
                    "offl_pch_excess",
                    "offl_winning_rate",
                    "lead_underwriter",
                    "lead_underwriter_vol",
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
    """下载 债券_可转债发行："""
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 下载数据存储的表格名称
    table_name = config["Ts_ZhaiQuan_KeZhuanZhaiFaXing"]["table_name"]

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
