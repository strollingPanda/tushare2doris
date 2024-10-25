import tushare as ts
import basis.basis_function
import basis.with_pydoris
from functools import wraps


def get_ts_code_of_convertible_bond():
    """下载 可转债转股结果 时，需提供转债代码。此函数用于获取转债代码"""
    # 连接database
    doris_client = basis.with_pydoris.connect_database()
    # 读取config/database.yaml
    config = basis.basis_function.load_config()
    option = (
        "SELECT ts_code FROM "
        + config["database_name"]
        + "."
        + config["Ts_ZhaiQuan_KeZhuanZhaiJiChuXinXi"]["table_name"]
        + ";"
    )
    ts_code_all_ori = doris_client.query(option)  # 从doris返回的数据是list，每个list里是一个tuple
    # 本函数最终返回一个list，每个list的代码数量等于或少于1000个（除最后一个list成员，前面的都为1000个）
    ts_code_all = []
    num_ts_code_each_member = 900  # 每个ts_code_all的list_member里，存放多少个ts_code
    for i in range(len(ts_code_all_ori)):  # 对于每一个ts_code
        # 当序号除以num_ts_code_each_member为0时，表示这是一个新的list member
        if (i % num_ts_code_each_member) == 0:
            list_member = ""
        list_member = list_member + ts_code_all_ori[i][0] + ","
        # 如果此时list_member已存放num_ts_code_each_member个ts_code，或当前已经是最后一个ts_code，则将这个list_member填入ts_code_all
        if (i % num_ts_code_each_member) == num_ts_code_each_member - 1 or i == (len(ts_code_all_ori) - 1):
            ts_code_all.append(list_member)
    return ts_code_all
