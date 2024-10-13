# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas

# https://tushare.pro/document/2?doc_id=46
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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_YeJiKuaiBao"]["table_name"]
        + """
        (	
            ts_code VARCHAR(50) COMMENT "TS股票代码",
            ann_date VARCHAR(8) COMMENT "公告日期",
            end_date VARCHAR(8) COMMENT "报告期",
            revenue DECIMAL COMMENT "营业收入(元)",
            operate_profit DECIMAL COMMENT "营业利润(元)",
            total_profit DECIMAL COMMENT "利润总额(元)",
            n_income DECIMAL COMMENT "净利润(元)",
            total_assets DECIMAL COMMENT "总资产(元)",
            total_hldr_eqy_exc_min_int DECIMAL COMMENT "股东权益合计(不含少数股东权益)(元)",
            diluted_eps DECIMAL COMMENT "每股收益(摊薄)(元)",
            diluted_roe DECIMAL COMMENT "净资产收益率(摊薄)(%)",
            yoy_net_profit DECIMAL COMMENT "去年同期修正后净利润",
            bps DECIMAL COMMENT "每股净资产",
            yoy_sales DECIMAL COMMENT "同比增长率:营业收入",
            yoy_op DECIMAL COMMENT "同比增长率:营业利润",
            yoy_tp DECIMAL COMMENT "同比增长率:利润总额",
            yoy_dedu_np DECIMAL COMMENT "同比增长率:归属母公司股东的净利润",
            yoy_eps DECIMAL COMMENT "同比增长率:基本每股收益",
            yoy_roe DECIMAL COMMENT "同比增减:加权平均净资产收益率",
            growth_assets DECIMAL COMMENT "比年初增长率:总资产",
            yoy_equity DECIMAL COMMENT "比年初增长率:归属母公司的股东权益",
            growth_bps DECIMAL COMMENT "比年初增长率:归属于母公司股东的每股净资产",
            or_last_year DECIMAL COMMENT "去年同期营业收入",
            op_last_year DECIMAL COMMENT "去年同期营业利润",
            tp_last_year DECIMAL COMMENT "去年同期利润总额",
            np_last_year DECIMAL COMMENT "去年同期净利润",
            eps_last_year DECIMAL COMMENT "去年同期每股收益",
            open_net_assets DECIMAL COMMENT "期初净资产",
            open_bps DECIMAL COMMENT "期初每股净资产",
            perf_summary STRING COMMENT "业绩简要说明",
            is_audit BIGINT COMMENT "是否审计： 1是 0否",
            remark STRING COMMENT "备注",
            update_flag VARCHAR(1) COMMENT "更新标志",
        )
        UNIQUE KEY(
                ts_code,
                ann_date,
                end_date,
                revenue,
                operate_profit,
                total_profit,
                n_income,
                total_assets,
                total_hldr_eqy_exc_min_int,
                diluted_eps,
                diluted_roe,
                yoy_net_profit,
                bps,
                yoy_sales,
                yoy_op,
                yoy_tp,
                yoy_dedu_np,
                yoy_eps,
                yoy_roe,
                growth_assets,
                yoy_equity,
                growth_bps,
                or_last_year,
                op_last_year,
                tp_last_year,
                np_last_year,
                eps_last_year,
                open_net_assets,
                open_bps)
        COMMENT "tushare_沪深股票_财务数据_业绩快报"

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
        df = pro.express_vip(
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
                "revenue",
                "operate_profit",
                "total_profit",
                "n_income",
                "total_assets",
                "total_hldr_eqy_exc_min_int",
                "diluted_eps",
                "diluted_roe",
                "yoy_net_profit",
                "bps",
                "yoy_sales",
                "yoy_op",
                "yoy_tp",
                "yoy_dedu_np",
                "yoy_eps",
                "yoy_roe",
                "growth_assets",
                "yoy_equity",
                "growth_bps",
                "or_last_year",
                "op_last_year",
                "tp_last_year",
                "np_last_year",
                "eps_last_year",
                "open_net_assets",
                "open_bps",
                "perf_summary",
                "is_audit",
                "remark",
                "update_flag",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 沪深股票_财务数据_业绩快报"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_YeJiKuaiBao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # # 按公告日期下载
    # basis.basis_function.download_by_ann_date(table_name, download_execute, logger)

    # 按报告期下载
    basis.HuShunGuPiao_function.download_by_period(table_name, download_execute, logger)


if __name__ == "__main__":
    create_table()
    download()
