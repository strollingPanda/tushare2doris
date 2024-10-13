# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=33
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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_LiRunBiao"]["table_name"]
        + """
        (
            ts_code VARCHAR(50) COMMENT "TS代码",
            ann_date VARCHAR(8) COMMENT "公告日期",
            f_ann_date VARCHAR(8) COMMENT "实际公告日期",
            end_date VARCHAR(8) COMMENT "报告期",
            report_type VARCHAR(2) COMMENT "报告类型 1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表 6母公司报表 7母公司单季表 8 母公司调整单季表 9母公司调整表 10母公司调整前报表 11调整前合并报表 12母公司调整前报表",
            comp_type VARCHAR(1) COMMENT "公司类型(1一般工商业2银行3保险4证券)",
            end_type VARCHAR(8) COMMENT "报告期类型",
            basic_eps DECIMAL COMMENT "基本每股收益",
            diluted_eps DECIMAL COMMENT "稀释每股收益",
            total_revenue DECIMAL COMMENT "营业总收入",
            revenue DECIMAL COMMENT "营业收入",
            int_income DECIMAL COMMENT "利息收入",
            prem_earned DECIMAL COMMENT "已赚保费",
            comm_income DECIMAL COMMENT "手续费及佣金收入",
            n_commis_income DECIMAL COMMENT "手续费及佣金净收入",
            n_oth_income DECIMAL COMMENT "其他经营净收益",
            n_oth_b_income DECIMAL COMMENT "加:其他业务净收益",
            prem_income DECIMAL COMMENT "保险业务收入",
            out_prem DECIMAL COMMENT "减:分出保费",
            une_prem_reser DECIMAL COMMENT "提取未到期责任准备金",
            reins_income DECIMAL COMMENT "其中:分保费收入",
            n_sec_tb_income DECIMAL COMMENT "代理买卖证券业务净收入",
            n_sec_uw_income DECIMAL COMMENT "证券承销业务净收入",
            n_asset_mg_income DECIMAL COMMENT "受托客户资产管理业务净收入",
            oth_b_income DECIMAL COMMENT "其他业务收入",
            fv_value_chg_gain DECIMAL COMMENT "加:公允价值变动净收益",
            invest_income DECIMAL COMMENT "加:投资净收益",
            ass_invest_income DECIMAL COMMENT "其中:对联营企业和合营企业的投资收益",
            forex_gain DECIMAL COMMENT "加:汇兑净收益",
            total_cogs DECIMAL COMMENT "营业总成本",
            oper_cost DECIMAL COMMENT "减:营业成本",
            int_exp DECIMAL COMMENT "减:利息支出",
            comm_exp DECIMAL COMMENT "减:手续费及佣金支出",
            biz_tax_surchg DECIMAL COMMENT "减:营业税金及附加",
            sell_exp DECIMAL COMMENT "减:销售费用",
            admin_exp DECIMAL COMMENT "减:管理费用",
            fin_exp DECIMAL COMMENT "减:财务费用",
            assets_impair_loss DECIMAL COMMENT "减:资产减值损失",
            prem_refund DECIMAL COMMENT "退保金",
            compens_payout DECIMAL COMMENT "赔付总支出",
            reser_insur_liab DECIMAL COMMENT "提取保险责任准备金",
            div_payt DECIMAL COMMENT "保户红利支出",
            reins_exp DECIMAL COMMENT "分保费用",
            oper_exp DECIMAL COMMENT "营业支出",
            compens_payout_refu DECIMAL COMMENT "减:摊回赔付支出",
            insur_reser_refu DECIMAL COMMENT "减:摊回保险责任准备金",
            reins_cost_refund DECIMAL COMMENT "减:摊回分保费用",
            other_bus_cost DECIMAL COMMENT "其他业务成本",
            operate_profit DECIMAL COMMENT "营业利润",
            non_oper_income DECIMAL COMMENT "加:营业外收入",
            non_oper_exp DECIMAL COMMENT "减:营业外支出",
            nca_disploss DECIMAL COMMENT "其中:减:非流动资产处置净损失",
            total_profit DECIMAL COMMENT "利润总额",
            income_tax DECIMAL COMMENT "所得税费用",
            n_income DECIMAL COMMENT "净利润(含少数股东损益)",
            n_income_attr_p DECIMAL COMMENT "净利润(不含少数股东损益)",
            minority_gain DECIMAL COMMENT "少数股东损益",
            oth_compr_income DECIMAL COMMENT "其他综合收益",
            t_compr_income DECIMAL COMMENT "综合收益总额",
            compr_inc_attr_p DECIMAL COMMENT "归属于母公司(或股东)的综合收益总额",
            compr_inc_attr_m_s DECIMAL COMMENT "归属于少数股东的综合收益总额",
            ebit DECIMAL COMMENT "息税前利润",
            ebitda DECIMAL COMMENT "息税折旧摊销前利润",
            insurance_exp DECIMAL COMMENT "保险业务支出",
            undist_profit DECIMAL COMMENT "年初未分配利润",
            distable_profit DECIMAL COMMENT "可分配利润",
            rd_exp DECIMAL COMMENT "研发费用",
            fin_exp_int_exp DECIMAL COMMENT "财务费用:利息费用",
            fin_exp_int_inc DECIMAL COMMENT "财务费用:利息收入",
            transfer_surplus_rese DECIMAL COMMENT "盈余公积转入",
            transfer_housing_imprest DECIMAL COMMENT "住房周转金转入",
            transfer_oth DECIMAL COMMENT "其他转入",
            adj_lossgain DECIMAL COMMENT "调整以前年度损益",
            withdra_legal_surplus DECIMAL COMMENT "提取法定盈余公积",
            withdra_legal_pubfund DECIMAL COMMENT "提取法定公益金",
            withdra_biz_devfund DECIMAL COMMENT "提取企业发展基金",
            withdra_rese_fund DECIMAL COMMENT "提取储备基金",
            withdra_oth_ersu DECIMAL COMMENT "提取任意盈余公积金",
            workers_welfare DECIMAL COMMENT "职工奖金福利",
            distr_profit_shrhder DECIMAL COMMENT "可供股东分配的利润",
            prfshare_payable_dvd DECIMAL COMMENT "应付优先股股利",
            comshare_payable_dvd DECIMAL COMMENT "应付普通股股利",
            capit_comstock_div DECIMAL COMMENT "转作股本的普通股股利",
            net_after_nr_lp_correct DECIMAL COMMENT "扣除非经常性损益后的净利润（更正前）",
            oth_income DECIMAL COMMENT "其他收益",
            asset_disp_income DECIMAL COMMENT "资产处置收益",
            continued_net_profit DECIMAL COMMENT "持续经营净利润",
            end_net_profit DECIMAL COMMENT "终止经营净利润",
            credit_impa_loss DECIMAL COMMENT "信用减值损失",
            net_expo_hedging_benefits DECIMAL COMMENT "净敞口套期收益",
            oth_impair_loss_assets DECIMAL COMMENT "其他资产减值损失",
            total_opcost DECIMAL COMMENT "营业总成本2",
            amodcost_fin_assets DECIMAL COMMENT "以摊余成本计量的金融资产终止确认收益",
            update_flag VARCHAR(1) COMMENT "更新标识",
        )
        UNIQUE KEY(
                    ts_code,
                    ann_date,
                    f_ann_date,
                    end_date,
                    report_type,
                    comp_type,
                    end_type,
                    basic_eps,
                    diluted_eps,
                    total_revenue,
                    revenue,
                    int_income,
                    prem_earned,
                    comm_income,
                    n_commis_income,
                    n_oth_income,
                    n_oth_b_income,
                    prem_income,
                    out_prem,
                    une_prem_reser,
                    reins_income,
                    n_sec_tb_income,
                    n_sec_uw_income,
                    n_asset_mg_income,
                    oth_b_income,
                    fv_value_chg_gain,
                    invest_income,
                    ass_invest_income,
                    forex_gain,
                    total_cogs,
                    oper_cost,
                    int_exp,
                    comm_exp,
                    biz_tax_surchg,
                    sell_exp,
                    admin_exp,
                    fin_exp,
                    assets_impair_loss,
                    prem_refund,
                    compens_payout,
                    reser_insur_liab,
                    div_payt,
                    reins_exp,
                    oper_exp,
                    compens_payout_refu,
                    insur_reser_refu,
                    reins_cost_refund,
                    other_bus_cost,
                    operate_profit,
                    non_oper_income,
                    non_oper_exp,
                    nca_disploss,
                    total_profit,
                    income_tax,
                    n_income,
                    n_income_attr_p,
                    minority_gain,
                    oth_compr_income,
                    t_compr_income,
                    compr_inc_attr_p,
                    compr_inc_attr_m_s,
                    ebit,
                    ebitda,
                    insurance_exp,
                    undist_profit,
                    distable_profit,
                    rd_exp,
                    fin_exp_int_exp,
                    fin_exp_int_inc,
                    transfer_surplus_rese,
                    transfer_housing_imprest,
                    transfer_oth,
                    adj_lossgain,
                    withdra_legal_surplus,
                    withdra_legal_pubfund,
                    withdra_biz_devfund,
                    withdra_rese_fund,
                    withdra_oth_ersu,
                    workers_welfare,
                    distr_profit_shrhder,
                    prfshare_payable_dvd,
                    comshare_payable_dvd,
                    capit_comstock_div,
                    net_after_nr_lp_correct,
                    oth_income,
                    asset_disp_income,
                    continued_net_profit,
                    end_net_profit,
                    credit_impa_loss,
                    net_expo_hedging_benefits,
                    oth_impair_loss_assets,
                    total_opcost,
                    amodcost_fin_assets,
                    update_flag)
        COMMENT "tushare_沪深股票_财务数据_利润表"

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
    f_ann_date="",
    start_date="",
    end_date="",
    period="",
    report_type="",
    comp_type="",
    is_calc="",
    limit=8000,
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
            df_local = pro.income_vip(
                **{
                    "ts_code": ts_code,
                    "ann_date": ann_date,
                    "f_ann_date": f_ann_date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "period": period,
                    "report_type": report_type,
                    "comp_type": comp_type,
                    "is_calc": is_calc,
                    "limit": limit,
                    "offset": offset,
                },
                fields=[
                    "ts_code",
                    "ann_date",
                    "f_ann_date",
                    "end_date",
                    "report_type",
                    "comp_type",
                    "end_type",
                    "basic_eps",
                    "diluted_eps",
                    "total_revenue",
                    "revenue",
                    "int_income",
                    "prem_earned",
                    "comm_income",
                    "n_commis_income",
                    "n_oth_income",
                    "n_oth_b_income",
                    "prem_income",
                    "out_prem",
                    "une_prem_reser",
                    "reins_income",
                    "n_sec_tb_income",
                    "n_sec_uw_income",
                    "n_asset_mg_income",
                    "oth_b_income",
                    "fv_value_chg_gain",
                    "invest_income",
                    "ass_invest_income",
                    "forex_gain",
                    "total_cogs",
                    "oper_cost",
                    "int_exp",
                    "comm_exp",
                    "biz_tax_surchg",
                    "sell_exp",
                    "admin_exp",
                    "fin_exp",
                    "assets_impair_loss",
                    "prem_refund",
                    "compens_payout",
                    "reser_insur_liab",
                    "div_payt",
                    "reins_exp",
                    "oper_exp",
                    "compens_payout_refu",
                    "insur_reser_refu",
                    "reins_cost_refund",
                    "other_bus_cost",
                    "operate_profit",
                    "non_oper_income",
                    "non_oper_exp",
                    "nca_disploss",
                    "total_profit",
                    "income_tax",
                    "n_income",
                    "n_income_attr_p",
                    "minority_gain",
                    "oth_compr_income",
                    "t_compr_income",
                    "compr_inc_attr_p",
                    "compr_inc_attr_m_s",
                    "ebit",
                    "ebitda",
                    "insurance_exp",
                    "undist_profit",
                    "distable_profit",
                    "rd_exp",
                    "fin_exp_int_exp",
                    "fin_exp_int_inc",
                    "transfer_surplus_rese",
                    "transfer_housing_imprest",
                    "transfer_oth",
                    "adj_lossgain",
                    "withdra_legal_surplus",
                    "withdra_legal_pubfund",
                    "withdra_biz_devfund",
                    "withdra_rese_fund",
                    "withdra_oth_ersu",
                    "workers_welfare",
                    "distr_profit_shrhder",
                    "prfshare_payable_dvd",
                    "comshare_payable_dvd",
                    "capit_comstock_div",
                    "net_after_nr_lp_correct",
                    "oth_income",
                    "asset_disp_income",
                    "continued_net_profit",
                    "end_net_profit",
                    "credit_impa_loss",
                    "net_expo_hedging_benefits",
                    "oth_impair_loss_assets",
                    "total_opcost",
                    "amodcost_fin_assets",
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
    """下载 沪深股票_财务数据_利润表"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_LiRunBiao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按报告期下载
    for report_type in range(1, 13):
        basis.HuShunGuPiao_function.download_financial_statement_by_period(
            table_name, download_execute, report_type, logger
        )


if __name__ == "__main__":
    import basis.basis_function

    basis.basis_function.create_database()  # 创建数据库
    create_table()
    download()
