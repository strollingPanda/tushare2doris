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

# https://tushare.pro/document/2?doc_id=155


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
        + config["Ts_QiHuo_NanHuaQiHuoZhiShuHangQing"]["table_name"]
        + """
        (
            ts_code VARCHAR(30) COMMENT "指数代码",
            trade_date VARCHAR(8) COMMENT "交易日",
            close DECIMAL COMMENT "收盘点位",
            open DECIMAL COMMENT "开盘点位",
            high DECIMAL COMMENT "最高点位",
            low DECIMAL COMMENT "最低点位",
            pre_close DECIMAL COMMENT "昨日收盘点",
            change DECIMAL COMMENT "涨跌点",
            pct_chg DECIMAL COMMENT "涨跌幅",
            vol DECIMAL COMMENT "成交量（手）",
            amount DECIMAL COMMENT "成交额（千元）",
        )
        UNIQUE KEY(ts_code,trade_date)
        COMMENT "tushare_期货_南华期货指数行情"

        DISTRIBUTED BY HASH(ts_code) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );"""
    )
    doris_client.execute(operation)


@basis.basis_function.retry
def download_execute(pro, logger, ts_code="", trade_date="", start_date="", end_date="", limit="", offset=""):
    """
    执行具体下载操作
    """
    exception_status = 0  # 0表示下载正常，1表示出现异常状况
    try:
        df = pro.index_daily(
            **{
                "ts_code": ts_code,
                "trade_date": trade_date,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "offset": offset,
            },
            fields=[
                "ts_code",
                "trade_date",
                "close",
                "open",
                "high",
                "low",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ]
        )
    except:
        logger.error("An exception occurred")
        df = pandas.DataFrame()
        exception_status = 1  # 下载出现异常状况
    return df, exception_status


def download():
    """下载 期货-南华期货指数行情"""
    ts_code_all = {
        "NHAI.NH": "南华农产品指数",
        "NHCI.NH": "南华商品指数",
        "NHECI.NH": "南华能化指数",
        "NHFI.NH": "南华黑色指数",
        "NHII.NH": "南华工业品指数",
        "NHMI.NH": "南华金属指数",
        "NHNFI.NH": "南华有色金属",
        "NHPMI.NH": "南华贵金属指数",
        "A.NH": "南华连大豆指数",
        "AG.NH": "南华沪银指数",
        "AL.NH": "南华沪铝指数",
        "AP.NH": "南华郑苹果指数",
        "AU.NH": "南华沪黄金指数",
        "BB.NH": "南华连胶合板指数",
        "BU.NH": "南华沪石油沥青指数",
        "C.NH": "南华连玉米指数",
        "CF.NH": "南华郑棉花指数",
        "CS.NH": "南华连玉米淀粉指数",
        "CU.NH": "南华沪铜指数",
        "CY.NH": "南华棉纱指数",
        "ER.NH": "南华郑籼稻指数",
        "FB.NH": "南华连纤维板指数",
        "FG.NH": "南华郑玻璃指数",
        "FU.NH": "南华沪燃油指数",
        "HC.NH": "南华沪热轧卷板指数",
        "I.NH": "南华连铁矿石指数",
        "J.NH": "南华连焦炭指数",
        "JD.NH": "南华连鸡蛋指数",
        "JM.NH": "南华连焦煤指数",
        "JR.NH": "南华郑粳稻指数",
        "L.NH": "南华连乙烯指数",
        "LR.NH": "南华郑晚籼稻指数",
        "M.NH": "南华连豆粕指数",
        "ME.NH": "南华郑甲醇指数",
        "NI.NH": "南华沪镍指数",
        "P.NH": "南华连棕油指数",
        "PB.NH": "南华沪铅指数",
        "PP.NH": "南华连聚丙烯指数",
        "RB.NH": "南华沪螺钢指数",
        "RM.NH": "南华郑菜籽粕指数",
        "RO.NH": "南华郑菜油指数",
        "RS.NH": "南华郑油菜籽指数",
        "RU.NH": "南华沪天胶指数",
        "SC.NH": "南华原油指数",
        "SF.NH": "南华郑硅铁指数",
        "SM.NH": "南华郑锰硅指数",
        "SN.NH": "南华沪锡指数",
        "SP.NH": "南华纸浆指数",
        "SR.NH": "南华郑白糖指数",
        "TA.NH": "南华郑精对苯二甲酸指数",
        "TC.NH": "南华郑动力煤指数",
        "V.NH": "南华连聚氯乙烯指数",
        "WR.NH": "南华沪线材指数",
        "WS.NH": "南华郑强麦指数",
        "Y.NH": "南华连豆油指数",
        "ZN.NH": "南华沪锌指数",
    }
    # 连接tushare
    pro = basis.basis_function.connect_tushare()

    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 下载数据存储的表格名称
    table_name = config["Ts_QiHuo_NanHuaQiHuoZhiShuHangQing"]["table_name"]

    # 创建logger
    logger = basis.basis_function.creat_logger()

    # 下载每一个品种
    for ts_code_local in ts_code_all.keys():
        logger.info("downloading " + table_name + ". ts_code: " + ts_code_local)
        df = download_execute(pro, logger, ts_code=ts_code_local)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


if __name__ == "__main__":
    create_table()
    download()
