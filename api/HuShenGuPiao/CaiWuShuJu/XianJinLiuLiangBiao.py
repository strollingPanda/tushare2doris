# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools

import basis.with_pydoris
import basis.basis_function
import basis.HuShunGuPiao_function
import basis.with_pydoris
import pandas
import time

# https://tushare.pro/document/2?doc_id=44
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
        + config["Ts_HuShenGuPiao_CaiWuShuJu_XianJinLiuLiangBiao"]["table_name"]
        + """
        (   ts_code VARCHAR(50) COMMENT "TS股票代码",
            ann_date VARCHAR(8) COMMENT "公告日期",
            f_ann_date VARCHAR(8) COMMENT "实际公告日期",
            end_date VARCHAR(8) COMMENT "报告期",
            comp_type VARCHAR(1) COMMENT "公司类型(1一般工商业2银行3保险4证券)",
            report_type VARCHAR(2) COMMENT "报表类型",
            end_type VARCHAR(2) COMMENT "报告期类型",
            net_profit DECIMAL COMMENT "净利润",
            finan_exp DECIMAL COMMENT "财务费用",
            c_fr_sale_sg DECIMAL COMMENT "销售商品、提供劳务收到的现金",
            recp_tax_rends DECIMAL COMMENT "收到的税费返还",
            n_depos_incr_fi DECIMAL COMMENT "客户存款和同业存放款项净增加额",
            n_incr_loans_cb DECIMAL COMMENT "向中央银行借款净增加额",
            n_inc_borr_oth_fi DECIMAL COMMENT "向其他金融机构拆入资金净增加额",
            prem_fr_orig_contr DECIMAL COMMENT "收到原保险合同保费取得的现金",
            n_incr_insured_dep DECIMAL COMMENT "保户储金净增加额",
            n_reinsur_prem DECIMAL COMMENT "收到再保业务现金净额",
            n_incr_disp_tfa DECIMAL COMMENT "处置交易性金融资产净增加额",
            ifc_cash_incr DECIMAL COMMENT "收取利息和手续费净增加额",
            n_incr_disp_faas DECIMAL COMMENT "处置可供出售金融资产净增加额",
            n_incr_loans_oth_bank DECIMAL COMMENT "拆入资金净增加额",
            n_cap_incr_repur DECIMAL COMMENT "回购业务资金净增加额",
            c_fr_oth_operate_a DECIMAL COMMENT "收到其他与经营活动有关的现金",
            c_inf_fr_operate_a DECIMAL COMMENT "经营活动现金流入小计",
            c_paid_goods_s DECIMAL COMMENT "购买商品、接受劳务支付的现金",
            c_paid_to_for_empl DECIMAL COMMENT "支付给职工以及为职工支付的现金",
            c_paid_for_taxes DECIMAL COMMENT "支付的各项税费",
            n_incr_clt_loan_adv DECIMAL COMMENT "客户贷款及垫款净增加额",
            n_incr_dep_cbob DECIMAL COMMENT "存放央行和同业款项净增加额",
            c_pay_claims_orig_inco DECIMAL COMMENT "支付原保险合同赔付款项的现金",
            pay_handling_chrg DECIMAL COMMENT "支付手续费的现金",
            pay_comm_insur_plcy DECIMAL COMMENT "支付保单红利的现金",
            oth_cash_pay_oper_act DECIMAL COMMENT "支付其他与经营活动有关的现金",
            st_cash_out_act DECIMAL COMMENT "经营活动现金流出小计",
            n_cashflow_act DECIMAL COMMENT "经营活动产生的现金流量净额",
            oth_recp_ral_inv_act DECIMAL COMMENT "收到其他与投资活动有关的现金",
            c_disp_withdrwl_invest DECIMAL COMMENT "收回投资收到的现金",
            c_recp_return_invest DECIMAL COMMENT "取得投资收益收到的现金",
            n_recp_disp_fiolta DECIMAL COMMENT "处置固定资产、无形资产和其他长期资产收回的现金净额",
            n_recp_disp_sobu DECIMAL COMMENT "处置子公司及其他营业单位收到的现金净额",
            stot_inflows_inv_act DECIMAL COMMENT "投资活动现金流入小计",
            c_pay_acq_const_fiolta DECIMAL COMMENT "购建固定资产、无形资产和其他长期资产支付的现金",
            c_paid_invest DECIMAL COMMENT "投资支付的现金",
            n_disp_subs_oth_biz DECIMAL COMMENT "取得子公司及其他营业单位支付的现金净额",
            oth_pay_ral_inv_act DECIMAL COMMENT "支付其他与投资活动有关的现金",
            n_incr_pledge_loan DECIMAL COMMENT "质押贷款净增加额",
            stot_out_inv_act DECIMAL COMMENT "投资活动现金流出小计",
            n_cashflow_inv_act DECIMAL COMMENT "投资活动产生的现金流量净额",
            c_recp_borrow DECIMAL COMMENT "取得借款收到的现金",
            proc_issue_bonds DECIMAL COMMENT "发行债券收到的现金",
            oth_cash_recp_ral_fnc_act DECIMAL COMMENT "收到其他与筹资活动有关的现金",
            stot_cash_in_fnc_act DECIMAL COMMENT "筹资活动现金流入小计",
            free_cashflow DECIMAL COMMENT "企业自由现金流量",
            c_prepay_amt_borr DECIMAL COMMENT "偿还债务支付的现金",
            c_pay_dist_dpcp_int_exp DECIMAL COMMENT "分配股利、利润或偿付利息支付的现金",
            incl_dvd_profit_paid_sc_ms DECIMAL COMMENT "其中:子公司支付给少数股东的股利、利润",
            oth_cashpay_ral_fnc_act DECIMAL COMMENT "支付其他与筹资活动有关的现金",
            stot_cashout_fnc_act DECIMAL COMMENT "筹资活动现金流出小计",
            n_cash_flows_fnc_act DECIMAL COMMENT "筹资活动产生的现金流量净额",
            eff_fx_flu_cash DECIMAL COMMENT "汇率变动对现金的影响",
            n_incr_cash_cash_equ DECIMAL COMMENT "现金及现金等价物净增加额",
            c_cash_equ_beg_period DECIMAL COMMENT "期初现金及现金等价物余额",
            c_cash_equ_end_period DECIMAL COMMENT "期末现金及现金等价物余额",
            c_recp_cap_contrib DECIMAL COMMENT "吸收投资收到的现金",
            incl_cash_rec_saims DECIMAL COMMENT "其中:子公司吸收少数股东投资收到的现金",
            uncon_invest_loss DECIMAL COMMENT "未确认投资损失",
            prov_depr_assets DECIMAL COMMENT "加:资产减值准备",
            depr_fa_coga_dpba DECIMAL COMMENT "固定资产折旧、油气资产折耗、生产性生物资产折旧",
            amort_intang_assets DECIMAL COMMENT "无形资产摊销",
            lt_amort_deferred_exp DECIMAL COMMENT "长期待摊费用摊销",
            decr_deferred_exp DECIMAL COMMENT "待摊费用减少",
            incr_acc_exp DECIMAL COMMENT "预提费用增加",
            loss_disp_fiolta DECIMAL COMMENT "处置固定、无形资产和其他长期资产的损失",
            loss_scr_fa DECIMAL COMMENT "固定资产报废损失",
            loss_fv_chg DECIMAL COMMENT "公允价值变动损失",
            invest_loss DECIMAL COMMENT "投资损失",
            decr_def_inc_tax_assets DECIMAL COMMENT "递延所得税资产减少",
            incr_def_inc_tax_liab DECIMAL COMMENT "递延所得税负债增加",
            decr_inventories DECIMAL COMMENT "存货的减少",
            decr_oper_payable DECIMAL COMMENT "经营性应收项目的减少",
            incr_oper_payable DECIMAL COMMENT "经营性应付项目的增加",
            others DECIMAL COMMENT "其他",
            im_net_cashflow_oper_act DECIMAL COMMENT "经营活动产生的现金流量净额(间接法)",
            conv_debt_into_cap DECIMAL COMMENT "债务转为资本",
            conv_copbonds_due_within_1y DECIMAL COMMENT "一年内到期的可转换公司债券",
            fa_fnc_leases DECIMAL COMMENT "融资租入固定资产",
            im_n_incr_cash_equ DECIMAL COMMENT "现金及现金等价物净增加额(间接法)",
            net_dism_capital_add DECIMAL COMMENT "拆出资金净增加额",
            net_cash_rece_sec DECIMAL COMMENT "代理买卖证券收到的现金净额(元)",
            credit_impa_loss DECIMAL COMMENT "信用减值损失",
            use_right_asset_dep DECIMAL COMMENT "使用权资产折旧",
            oth_loss_asset DECIMAL COMMENT "其他资产减值损失",
            end_bal_cash DECIMAL COMMENT "现金的期末余额",
            beg_bal_cash DECIMAL COMMENT "减:现金的期初余额",
            end_bal_cash_equ DECIMAL COMMENT "加:现金等价物的期末余额",
            beg_bal_cash_equ DECIMAL COMMENT "减:现金等价物的期初余额",
            update_flag VARCHAR(1) COMMENT "更新标志",
        )
        UNIQUE KEY(
                    ts_code,
                    ann_date,
                    f_ann_date,
                    end_date,
                    comp_type,
                    report_type,
                    end_type,
                    net_profit,
                    finan_exp,
                    c_fr_sale_sg,
                    recp_tax_rends,
                    n_depos_incr_fi,
                    n_incr_loans_cb,
                    n_inc_borr_oth_fi,
                    prem_fr_orig_contr,
                    n_incr_insured_dep,
                    n_reinsur_prem,
                    n_incr_disp_tfa,
                    ifc_cash_incr,
                    n_incr_disp_faas,
                    n_incr_loans_oth_bank,
                    n_cap_incr_repur,
                    c_fr_oth_operate_a,
                    c_inf_fr_operate_a,
                    c_paid_goods_s,
                    c_paid_to_for_empl,
                    c_paid_for_taxes,
                    n_incr_clt_loan_adv,
                    n_incr_dep_cbob,
                    c_pay_claims_orig_inco,
                    pay_handling_chrg,
                    pay_comm_insur_plcy,
                    oth_cash_pay_oper_act,
                    st_cash_out_act,
                    n_cashflow_act,
                    oth_recp_ral_inv_act,
                    c_disp_withdrwl_invest,
                    c_recp_return_invest,
                    n_recp_disp_fiolta,
                    n_recp_disp_sobu,
                    stot_inflows_inv_act,
                    c_pay_acq_const_fiolta,
                    c_paid_invest,
                    n_disp_subs_oth_biz,
                    oth_pay_ral_inv_act,
                    n_incr_pledge_loan,
                    stot_out_inv_act,
                    n_cashflow_inv_act,
                    c_recp_borrow,
                    proc_issue_bonds,
                    oth_cash_recp_ral_fnc_act,
                    stot_cash_in_fnc_act,
                    free_cashflow,
                    c_prepay_amt_borr,
                    c_pay_dist_dpcp_int_exp,
                    incl_dvd_profit_paid_sc_ms,
                    oth_cashpay_ral_fnc_act,
                    stot_cashout_fnc_act,
                    n_cash_flows_fnc_act,
                    eff_fx_flu_cash,
                    n_incr_cash_cash_equ,
                    c_cash_equ_beg_period,
                    c_cash_equ_end_period,
                    c_recp_cap_contrib,
                    incl_cash_rec_saims,
                    uncon_invest_loss,
                    prov_depr_assets,
                    depr_fa_coga_dpba,
                    amort_intang_assets,
                    lt_amort_deferred_exp,
                    decr_deferred_exp,
                    incr_acc_exp,
                    loss_disp_fiolta,
                    loss_scr_fa,
                    loss_fv_chg,
                    invest_loss,
                    decr_def_inc_tax_assets,
                    incr_def_inc_tax_liab,
                    decr_inventories,
                    decr_oper_payable,
                    incr_oper_payable,
                    others,
                    im_net_cashflow_oper_act,
                    conv_debt_into_cap,
                    conv_copbonds_due_within_1y,
                    fa_fnc_leases,
                    im_n_incr_cash_equ,
                    net_dism_capital_add,
                    net_cash_rece_sec,
                    credit_impa_loss,
                    use_right_asset_dep,
                    oth_loss_asset,
                    end_bal_cash,
                    beg_bal_cash,
                    end_bal_cash_equ,
                    beg_bal_cash_equ,
                    update_flag)
        COMMENT "tushare_沪深股票_财务数据_现金流量表"

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
            df_local = pro.cashflow_vip(
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
                    "comp_type",
                    "report_type",
                    "end_type",
                    "net_profit",
                    "finan_exp",
                    "c_fr_sale_sg",
                    "recp_tax_rends",
                    "n_depos_incr_fi",
                    "n_incr_loans_cb",
                    "n_inc_borr_oth_fi",
                    "prem_fr_orig_contr",
                    "n_incr_insured_dep",
                    "n_reinsur_prem",
                    "n_incr_disp_tfa",
                    "ifc_cash_incr",
                    "n_incr_disp_faas",
                    "n_incr_loans_oth_bank",
                    "n_cap_incr_repur",
                    "c_fr_oth_operate_a",
                    "c_inf_fr_operate_a",
                    "c_paid_goods_s",
                    "c_paid_to_for_empl",
                    "c_paid_for_taxes",
                    "n_incr_clt_loan_adv",
                    "n_incr_dep_cbob",
                    "c_pay_claims_orig_inco",
                    "pay_handling_chrg",
                    "pay_comm_insur_plcy",
                    "oth_cash_pay_oper_act",
                    "st_cash_out_act",
                    "n_cashflow_act",
                    "oth_recp_ral_inv_act",
                    "c_disp_withdrwl_invest",
                    "c_recp_return_invest",
                    "n_recp_disp_fiolta",
                    "n_recp_disp_sobu",
                    "stot_inflows_inv_act",
                    "c_pay_acq_const_fiolta",
                    "c_paid_invest",
                    "n_disp_subs_oth_biz",
                    "oth_pay_ral_inv_act",
                    "n_incr_pledge_loan",
                    "stot_out_inv_act",
                    "n_cashflow_inv_act",
                    "c_recp_borrow",
                    "proc_issue_bonds",
                    "oth_cash_recp_ral_fnc_act",
                    "stot_cash_in_fnc_act",
                    "free_cashflow",
                    "c_prepay_amt_borr",
                    "c_pay_dist_dpcp_int_exp",
                    "incl_dvd_profit_paid_sc_ms",
                    "oth_cashpay_ral_fnc_act",
                    "stot_cashout_fnc_act",
                    "n_cash_flows_fnc_act",
                    "eff_fx_flu_cash",
                    "n_incr_cash_cash_equ",
                    "c_cash_equ_beg_period",
                    "c_cash_equ_end_period",
                    "c_recp_cap_contrib",
                    "incl_cash_rec_saims",
                    "uncon_invest_loss",
                    "prov_depr_assets",
                    "depr_fa_coga_dpba",
                    "amort_intang_assets",
                    "lt_amort_deferred_exp",
                    "decr_deferred_exp",
                    "incr_acc_exp",
                    "loss_disp_fiolta",
                    "loss_scr_fa",
                    "loss_fv_chg",
                    "invest_loss",
                    "decr_def_inc_tax_assets",
                    "incr_def_inc_tax_liab",
                    "decr_inventories",
                    "decr_oper_payable",
                    "incr_oper_payable",
                    "others",
                    "im_net_cashflow_oper_act",
                    "conv_debt_into_cap",
                    "conv_copbonds_due_within_1y",
                    "fa_fnc_leases",
                    "im_n_incr_cash_equ",
                    "net_dism_capital_add",
                    "net_cash_rece_sec",
                    "credit_impa_loss",
                    "use_right_asset_dep",
                    "oth_loss_asset",
                    "end_bal_cash",
                    "beg_bal_cash",
                    "end_bal_cash_equ",
                    "beg_bal_cash_equ",
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
    """下载 沪深股票_财务数据_现金流量表"""

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_HuShenGuPiao_CaiWuShuJu_XianJinLiuLiangBiao"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 按报告期下载
    # 报告类型 1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表 6母公司报表 7母公司单季表 8 母公司调整单季表 9母公司调整表 10母公司调整前报表 11调整前合并报表 12母公司调整前报表
    for report_type in range(1, 13):
        basis.HuShunGuPiao_function.download_financial_statement_by_period(
            table_name, download_execute, report_type, logger
        )


if __name__ == "__main__":
    create_table()
    download()
