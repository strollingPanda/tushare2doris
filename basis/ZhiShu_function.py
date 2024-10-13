import datetime
import tushare as ts
import basis.basis_function
import basis.with_pydoris
import yaml
import time
from functools import wraps
import pandas


def get_ts_code_of_index_by_market(market):
    """根据输入的市场，获取该市场全部指数代码"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    option = (
        "SELECT ts_code FROM "
        + config["database_name"]
        + "."
        + config["Ts_ZhiShu_ZhiShuJiBenXinXi"]["table_name"]
        + ' WHERE market = "'
        + market
        + '";'
    )
    ts_code_all_ori = doris_client.query(option)  # 从doris返回的数据是list，每个list里是一个tuple
    ts_code_all = []  # 本函数最终返回一个list，list里的每一个元素是字符串
    for tuple_local in ts_code_all_ori:
        ts_code_all.append(tuple_local[0])
    return ts_code_all


def download_index_daily_by_ts_code(table_name, download_execute, ts_code, logger):
    """按指数代码下载日线行情"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]
    # table_name里现存的最晚日期
    option = (
        "SELECT MAX(trade_date) FROM "
        + config["database_name"]
        + "."
        + table_name
        + ' WHERE ts_code = "'
        + ts_code
        + '";'
    )
    date_exist_latest = doris_client.query(option)[0][0]
    if download_as_fresh == 1:  # 如果download_as_fresh为1，则重新下载所有数据
        start_date = ""
    else:  # 如果download_as_fresh不为1
        if date_exist_latest is None:  # 如果数据库中还没有数据，则下载所有数据
            start_date = ""
        else:  # 如果数据库中还没数据，则从现存最晚数据下一天开始下载
            start_date = basis.basis_function.get_date_next(date_exist_latest)

    # 计算下载的结束日期
    # Ts_ZhiShu_ZhiShuJiBenXinXi 指数-指数基本信息 里该指数的里终止日期
    option = (
        "SELECT exp_date FROM "
        + config["database_name"]
        + '.Ts_ZhiShu_ZhiShuJiBenXinXi WHERE ts_code = "'
        + ts_code
        + '";'
    )
    date_exp_date = doris_client.query(option)[0][0]  # 数据库中存储的该指数的终止日期
    if date_exp_date is None:  # 如果数据库中没有终止日期，则下载结束日期为今日
        end_date = datetime.datetime.today().strftime("%Y%m%d")  # 设置下载结束日期为今日
    else:  # 如果数据库中有终止日期，则下载结束日期为今日和终止日期两者中较小的一个
        end_date = min(datetime.datetime.today().strftime("%Y%m%d"), date_exp_date)

    logger.info(
        "downloading "
        + table_name
        + ". ts_code:"
        + ts_code
        + ". start_date: "
        + start_date
        + ". end_date: "
        + end_date
    )
    # 如果开始日期小于等于结束日期
    if start_date <= end_date:
        df = download_execute(
            pro, logger, ts_code=ts_code, start_date=start_date, end_date=end_date
        )  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


def get_date_all_from_index_daily(ts_code):
    """获取某一个指数的全部交易日期"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    # 获取index_daily里的所有日期
    option = (
        "SELECT trade_date FROM "
        + config["database_name"]
        + '.Ts_ZhiShu_ZhiShuRiXianHangQing WHERE ts_code = "'
        + ts_code
        + '" ORDER BY trade_date ASC;'
    )
    date_all_ori = doris_client.query(option)  # 直接下载数据
    date_all = []
    for date_local in date_all_ori:
        date_all.append(date_local[0])
    return date_all  # 返回的数据为一个list，每一个元素为表示日期的字符串


def download_index_weight(table_name, download_execute, index_code, logger):
    """下载指数成分和权重"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]

    date_all = get_date_all_from_index_daily(index_code)  # 日线行情里存在的所有日期

    # table_name里现存的最晚日期
    option = (
        "SELECT MAX(trade_date) FROM "
        + config["database_name"]
        + "."
        + table_name
        + ' WHERE index_code = "'
        + index_code
        + '";'
    )
    date_exist_latest = doris_client.query(option)[0][0]

    if download_as_fresh == 1:  # 如果download_as_fresh为1，则重新下载所有数据
        date_all_to_download = date_all
    else:  # 如果download_as_fresh不为1
        if date_exist_latest is None:  # 如果数据库中还没有数据，则重新下载所有数据
            date_all_to_download = date_all
        else:  # 如果已经存在数据，则从现存最晚数据的下一天开始下载
            # 现存指数成分的最晚的日期在date_all中的多少行
            row_date_latest_in_all = basis.basis_function.get_row_in_list(date_exist_latest, date_all)
            date_all_to_download = date_all[row_date_latest_in_all + 1 :]
    if len(date_all_to_download) > 0:  # 如果需要进行下载
        start_date = min(date_all_to_download)  # 开始日期
        end_date = max(date_all_to_download)  # 结束日期
        logger.info(
            "downloading "
            + table_name
            + ". index_code:"
            + index_code
            + ". start_date: "
            + start_date
            + ". end_date: "
            + end_date
        )
        df = download_execute(
            pro, logger, index_code=index_code, start_date=start_date, end_date=end_date
        )  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载
