import basis.basis_function
import time
import datetime


def get_date_all_ann_date(start_date_str):
    """为按公告日期下载准备date_all,起始日期为start_date，截止日期为今天之后的若干日"""
    date_all = []
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")  # 将字符串转换为datetime
    end_date = datetime.datetime.today() + datetime.timedelta(days=10)  # 设置截止日期为今日之后的第10天
    while start_date <= end_date:
        date_all.append(start_date.strftime("%Y%m%d"))  # 将datetime转化为字符串
        start_date += datetime.timedelta(days=1)
    return date_all


def download_by_ann_date(table_name, download_execute, logger):
    """按公告日期下载"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]
    # 如果日线行情已经存在数据，从存在数据的最后一天向前算，重新下载几天的数据。
    # 注意，这样是因为担心最近几天doris中存在的数据可能有缺漏
    num_previous_days_redownload = config[table_name]["num_previous_days_redownload"]
    # first_date_download_default用来调节从哪天开始下载数据。默认是tushare数据库里的最早数据日期。
    first_date_download_default = config[table_name]["first_date_download_default"]

    # 为按公告日期下载准备date_all
    date_all = get_date_all_ann_date(first_date_download_default)

    # table_name里现存的最晚的ann_date
    option = "SELECT MAX(ann_date) FROM " + config["database_name"] + "." + table_name + ";"
    date_exist_latest = doris_client.query(option)[0][0]

    # 下载应从date_all的多少行开始执行。
    row_start_to_download = basis.basis_function.get_row_start_to_download(
        date_all, date_exist_latest, download_as_fresh, num_previous_days_redownload
    )

    # 这里的date_all是对于各自表单独生成的。
    # 因此计算可以直接计算date_all_to_download
    date_all_to_download = date_all[row_start_to_download:]  # 要下载的日期

    for ann_date_local in date_all_to_download:  # 执行下载
        logger.info("downloading " + table_name + ". ann_date: " + ann_date_local)
        df = download_execute(pro, logger, ann_date=ann_date_local)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


def download_by_f_ann_date(table_name, download_execute, logger):
    """按实际公告日期下载"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]
    # 如果日线行情已经存在数据，从存在数据的最后一天向前算，重新下载几天的数据。
    # 注意，这样是因为担心最近几天doris中存在的数据可能有缺漏
    num_previous_days_redownload = config[table_name]["num_previous_days_redownload"]
    # first_date_download_default用来调节从哪天开始下载数据。默认是tushare数据库里的最早数据日期。
    first_date_download_default = config[table_name]["first_date_download_default"]

    # 为按公告日期下载准备date_all。
    date_all = get_date_all_ann_date(first_date_download_default)

    # table_name里现存的最晚的ann_date
    option = "SELECT MAX(f_ann_date) FROM " + config["database_name"] + "." + table_name + ";"
    date_exist_latest = doris_client.query(option)[0][0]

    # 下载应从date_all的多少行开始执行。
    row_start_to_download = basis.basis_function.get_row_start_to_download(
        date_all, date_exist_latest, download_as_fresh, num_previous_days_redownload
    )

    # 这里的date_all是对于各自表单独生成的。
    # 因此计算可以直接计算date_all_to_download
    date_all_to_download = date_all[row_start_to_download:]  # 要下载的日期

    for f_ann_date_local in date_all_to_download:  # 执行下载
        logger.info("downloading " + table_name + ". ann_date: " + f_ann_date_local)
        df = download_execute(pro, logger, f_ann_date=f_ann_date_local)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


def get_date_all_period(first_date_download_default):

    # 获得从dateStart开始，至当日，每一个季度最后一天的日期
    # 开始日期
    year_start = int(first_date_download_default[0:4])
    month_start = int(first_date_download_default[4:6])
    day_start = int(first_date_download_default[6:8])

    date_start_num = datetime.date(year_start, month_start, day_start).toordinal()
    # 结束日期
    date_end_num = datetime.date.today().toordinal() + 100

    # 所有需要返回的季度末日期
    date_all = []

    for date_num_local in list(range(date_start_num, date_end_num + 1)):
        # 检查日期的年月日
        year_local = datetime.date.fromordinal(date_num_local).year
        month_local = datetime.date.fromordinal(date_num_local).month
        day_local = datetime.date.fromordinal(date_num_local).day

        # 如果当前的月日为季度末最后一日，则记录
        if (
            (month_local == 3 and day_local == 31)
            or (month_local == 6 and day_local == 30)
            or (month_local == 9 and day_local == 30)
            or (month_local == 12 and day_local == 31)
        ):
            date_all.append(str(year_local * 10000 + month_local * 100 + day_local))

    return date_all


def download_financial_statement_by_period(table_name, download_execute, report_type, logger):
    """按报告期下载三张财务报表，即利润表、资产负债表、现金流量表"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]
    # 重新下载之前几个季度的数据
    # 此处是按报告结束日期进行的下载。即对于年报，end_date是1231
    # 不同公司年报公布时间是不同的，且公布后可能出新公告修正，同时有些公司年报会推迟很久发布
    # 考虑到以上这些情况，在每次下载时，对之前几个季度的报告重新下载
    num_previous_seasons_redownload = config[table_name]["num_previous_seasons_redownload"]
    # first_date_download_default用来调节从哪天开始下载数据。默认是tushare数据库里的最早数据日期。
    first_date_download_default = config[table_name]["first_date_download_default"]

    # 为按报告期结束日期下载准备date_all
    date_all = get_date_all_period(first_date_download_default)

    # table_name里现存的最晚的报告期，在doris里存储的名字是end_date
    option = (
        "SELECT MAX(end_date) FROM "
        + config["database_name"]
        + "."
        + table_name
        + " WHERE report_type = "
        + str(report_type)
        + " ;"
    )
    date_exist_latest = doris_client.query(option)[0][0]

    # 下载应从date_all的多少行开始执行。
    row_start_to_download = basis.basis_function.get_row_start_to_download(
        date_all, date_exist_latest, download_as_fresh, num_previous_seasons_redownload
    )

    # 这里的date_all是对于各自表单独生成的。
    # 因此计算可以直接计算date_all_to_download
    date_all_to_download = date_all[row_start_to_download:]  # 要下载的日期

    for period in date_all_to_download:  # 执行下载
        logger.info("downloading " + table_name + ". period: " + period + " report_type: " + str(report_type))
        df = download_execute(pro, logger, period=period, report_type=report_type)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


def download_by_period(table_name, download_execute, logger):
    """按报告期下载"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]
    # 重新下载之前几个季度的数据
    # 此处是按报告结束日期进行的下载。即对于年报，end_date是1231
    # 不同公司年报公布时间是不同的，且公布后可能出新公告修正，同时有些公司年报会推迟很久发布
    # 考虑到以上这些情况，在每次下载时，对之前几个季度的报告重新下载
    num_previous_seasons_redownload = config[table_name]["num_previous_seasons_redownload"]
    # first_date_download_default用来调节从哪天开始下载数据。默认是tushare数据库里的最早数据日期。
    first_date_download_default = config[table_name]["first_date_download_default"]

    # 为按报告期结束日期下载准备date_all
    date_all = get_date_all_period(first_date_download_default)

    # table_name里现存的最晚的报告期，在doris里存储的名字是end_date
    option = "SELECT MAX(end_date) FROM " + config["database_name"] + "." + table_name + " ;"
    date_exist_latest = doris_client.query(option)[0][0]

    # 下载应从date_all的多少行开始执行。
    row_start_to_download = basis.basis_function.get_row_start_to_download(
        date_all, date_exist_latest, download_as_fresh, num_previous_seasons_redownload
    )

    # 这里的date_all是对于各自表单独生成的。
    # 因此计算可以直接计算date_all_to_download
    date_all_to_download = date_all[row_start_to_download:]  # 要下载的日期

    for period in date_all_to_download:  # 执行下载
        logger.info("downloading " + table_name + ". period: " + period)
        df = download_execute(pro, logger, period=period)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载


def download_by_end_date(table_name, download_execute, logger):
    """按财报周期下载"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 连接tushare
    pro = basis.basis_function.connect_tushare()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()

    # 对于日线行情，是否全部从头重新下载。如果为1，则从头重新下载；如果为0，则不从头重新下载
    download_as_fresh = config[table_name]["download_as_fresh"]
    # 重新下载之前几个季度的数据
    # 此处是按报告结束日期进行的下载。即对于年报，end_date是1231
    # 不同公司年报公布时间是不同的，且公布后可能出新公告修正，同时有些公司年报会推迟很久发布
    # 考虑到以上这些情况，在每次下载时，对之前几个季度的报告重新下载
    num_previous_seasons_redownload = config[table_name]["num_previous_seasons_redownload"]
    # first_date_download_default用来调节从哪天开始下载数据。默认是tushare数据库里的最早数据日期。
    first_date_download_default = config[table_name]["first_date_download_default"]

    # 为按报告期结束日期下载准备date_all
    date_all = get_date_all_period(first_date_download_default)

    # table_name里现存的最晚的报告期，在doris里存储的名字是end_date
    option = "SELECT MAX(end_date) FROM " + config["database_name"] + "." + table_name + " ;"
    date_exist_latest = doris_client.query(option)[0][0]

    # 下载应从date_all的多少行开始执行。
    row_start_to_download = basis.basis_function.get_row_start_to_download(
        date_all, date_exist_latest, download_as_fresh, num_previous_seasons_redownload
    )

    # 这里的date_all是对于各自表单独生成的。
    # 因此计算可以直接计算date_all_to_download
    date_all_to_download = date_all[row_start_to_download:]  # 要下载的日期

    for end_date in date_all_to_download:  # 执行下载
        logger.info("downloading " + table_name + ". end_date: " + end_date)
        df = download_execute(pro, logger, end_date=end_date)  # 执行下载
        basis.with_pydoris.upload_dataframe_as_json(df, table_name, logger)  # 上传至doris
        time.sleep(config["regular_gap"])  # 等待一段时间再继续下载
