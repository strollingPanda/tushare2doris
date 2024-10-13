import datetime
import tushare as ts
import basis.with_pydoris
import yaml
import time
from functools import wraps
from loguru import logger
import os


def load_config():
    """
    载入config/databases.yaml
    """
    with open("basis/config.yaml", "r", encoding="UTF-8") as file:
        config = yaml.safe_load(file)
    return config


def retry(func):
    """
    这是一个装饰器，当从tushare下载出错时重新下载，以确保用来确认从tushare下载了完整的信息。
    如果从tushare下载出错，且尝试下载次数少于最大约定次数(tried_times < retry_times_max)，则重新下载

    https://blog.csdn.net/wxy19980510/article/details/131373018?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522FA91862A-B65F-409F-A46D-92511AA3BAD2%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=FA91862A-B65F-409F-A46D-92511AA3BAD2&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~top_positive~default-1-131373018-null-null.142^v100^pc_search_result_base5&utm_term=%E8%A3%85%E9%A5%B0%E5%99%A8&spm=1018.2226.3001.4187
    """
    config = load_config()
    retry_times_max = config["retry_times_max"]  # 出错时最多重试次数
    retry_gap = config["retry_gap"]  # 重试间隔时间

    @wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal retry_times_max, retry_gap  # 出错时最多重试次数,重试间隔时间
        exception_status = 1  # exception_status为0表示下载正常，1表示出现异常状况。为了进行首次下载，设置为1
        tried_times = 0  # 已经尝试下载了几次
        while (
            exception_status == 1 and tried_times < retry_times_max
        ):  # 如果下载异常正常，同时尝试下次次数不足，重复下载
            if tried_times >= 1:  # 如果已经不是首次下载，暂停一段时间后再下载
                time.sleep(retry_gap)  # retry_gap秒后重新下载
            df, exception_status = func(*args, **kwargs)  # 执行下载
            tried_times += 1  # 记录已经下载了几次
            if exception_status == 0:  # 如果下载成功，则返回
                return df
        return df

    return wrapper


def create_database():
    """
    DorisClient内部在创建数据时的语句为
    "create database if not exists {database}"
    即若该数据库已存在，则不重复创建
    """
    doris_client = basis.with_pydoris.connect_doris()
    config = load_config()
    doris_client.create_database(config["database_name"])


def connect_tushare():
    """
    连接tushare。在本软件中，可以用以下两种方式给本地计算授权：
    1. 在 basis/config.yaml中，将token填入tushare_proToken
    2. 在python中运行
        import tushare as ts
        ts.set_token('your token here')
    https://tushare.pro/document/1?doc_id=40
    """
    # 读入tushare的config
    config = load_config()
    pro = ts.pro_api(config["tushare_proToken"])
    return pro


def cal_next_date_str(date_str_in):
    """
    根据输入的字符串形式的日期，返回该日期的下一天。返回值依然为字符串形式
    比如数据的'20230504'，返回值为'20230505'
    """
    date_str_out = (datetime.datetime.strptime(date_str_in, "%Y%m%d") + datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    )
    return date_str_out


def get_row_in_list(dataInput, data_list):
    """
    在data_list查找dataInput所在行数，并返回所在行数。如果不存在，返回None。
    这里的data_list考虑两种情况
    1.list的每一个元素是一个tuple，通常是sql语言SELECT * FROM...返回的结果
        如果SQL语句SELECT后只跟了一个column，则tuple只有一个元素
        在此函数里，dataInput只跟tuple的第一个元素取对比
    2. list的每一个元素是一个字符串
    """
    row_in_list = None
    for row_in_list in range(len(data_list)):
        # 如果list的元素是tuple
        if isinstance(data_list[row_in_list], tuple):
            if dataInput == data_list[row_in_list][0]:
                return row_in_list
        # 如果list的元素是str
        if isinstance(data_list[row_in_list], str):
            if dataInput == data_list[row_in_list]:
                return row_in_list
    return row_in_list


# 应从多少行开始下载
def get_row_start_to_download(
    date_all_JiaoYiRiLi, date_latest_exist, download_as_fresh, num_previous_days_redownload
):
    """
    下载应从date_all_JiaoYiRiLi的多少行开始执行。
    逻辑为：
    如果download_as_fresh为1，下载全部数据，即row_start_to_download = 0
    如果download_as_fresh为0
        如果日线行情的最晚日期为None，下载全部数据，即row_start_to_download = 0
        如果日线行情的最晚日期不是None，计算row_date_latest_in_all，即date_latest_exist在date_all_JiaoYiRiLi中的多少行
            如果(row_date_latest_in_all - num_previous_days_redownload+ 1)<=0，则下载全部数据，即row_start_to_download = 0
            否则row_start_to_download=row_date_latest_in_all-num_previous_days_redownload+1
    """
    if download_as_fresh == 1:
        row_start_to_download = 0
    if download_as_fresh == 0:
        if date_latest_exist is None:
            row_start_to_download = 0
        else:
            row_date_latest_in_all = basis.basis_function.get_row_in_list(
                date_latest_exist, date_all_JiaoYiRiLi
            )
            if (row_date_latest_in_all - num_previous_days_redownload + 1) <= 0:
                row_start_to_download = 0
            else:
                row_start_to_download = row_date_latest_in_all - num_previous_days_redownload + 1
    return row_start_to_download


def download_by_date(table_name, download_execute, asset_type, logger):
    """按照日期下载"""

    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = connect_tushare()
    # 读取config/database.yaml
    config = load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]
    # 如果日线行情已经存在数据，从存在数据的最后一天向前算，重新下载几天的数据。
    # 注意，这样是因为担心最近几天doris中存在的数据可能有缺漏
    num_previous_days_redownload = config[table_name]["num_previous_days_redownload"]
    # first_date_download_default用来调节从哪天开始下载数据。默认是tushare数据库里的最早数据日期。
    first_date_download_default = config[table_name]["first_date_download_default"]

    # 不同交易品种的 交易日历 表不同。
    # 指数中的沪深相关指数 用 沪深股票 的交易日历
    table_name_JiaoYiRiLi_dict = {
        "沪深股票": config["Ts_HuShenGuPiao_JiChuShuJu_JiaoYiRiLi"]["table_name"],
        "期货": config["Ts_QiHuo_JiaoYiRiLi"]["table_name"],
        "沪深指数": config["Ts_HuShenGuPiao_JiChuShuJu_JiaoYiRiLi"]["table_name"],
        "港股": config["Ts_GangGu_GangGuJiaoYiRiLi"]["table_name"],
    }
    table_name_JiaoYiRiLi = table_name_JiaoYiRiLi_dict[asset_type]
    # 交易日历里的有交易的日期
    option = (
        "SELECT DISTINCT cal_date FROM "
        + config["database_name"]
        + "."
        + table_name_JiaoYiRiLi
        + " WHERE is_open=1 ORDER BY cal_date ASC;"
    )
    date_all_JiaoYiRiLi = doris_client.query(option)  # 交易日历里的所有日期

    # table_name里现存的最晚日期
    option = "SELECT MAX(trade_date) FROM " + config["database_name"] + "." + table_name + ";"
    date_exist_latest = doris_client.query(option)[0][0]

    # 下载应从date_all_JiaoYiRiLi的多少行开始执行。
    row_start_to_download = get_row_start_to_download(
        date_all_JiaoYiRiLi, date_exist_latest, download_as_fresh, num_previous_days_redownload
    )
    # 要下载的日期不能早于first_date_download_default
    # 这里的逻辑是这样：
    # date_all_JiaoYiRiLi是对于所有股票或期货的，对于股票来说，是从1990年开始的，因此是一个特别长的list。row_to_download可能为0。
    # 但对于某些表，比如港股通，数据是从2014年开始的。
    # 因此在下载港股通数据时，对于1990-2014年之间的数据，可以不用尝试下载
    # 同时要下载的日期不要晚于明天
    date_tomorrow = get_date_next(datetime.datetime.today().strftime("%Y%m%d"))
    date_all_to_download = []
    for row_to_download in range(row_start_to_download, len(date_all_JiaoYiRiLi)):
        date_local = date_all_JiaoYiRiLi[row_to_download][0]  # 当前应该下载的日期
        if date_local >= first_date_download_default and date_local <= date_tomorrow:
            date_all_to_download.append(date_local)

    for trade_date_local in date_all_to_download:  # 执行下载
        logger.info("downloading " + table_name + ". trade_date: " + trade_date_local)
        df = download_execute(pro, logger, trade_date=trade_date_local)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


def get_date_next(date_input):
    """获取下一个日期。输入和输出均为YYYYmmdd格式的字符串"""
    date_input_num = datetime.datetime.strptime(date_input, "%Y%m%d")
    date_next_num = date_input_num + datetime.timedelta(days=1)
    date_next = date_next_num.strftime("%Y%m%d")
    return date_next


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)


def creat_logger():
    mkdir("log")
    logger.add("log/" + datetime.datetime.today().strftime("%Y%m%d") + ".log", retention="10 days")
    return logger
