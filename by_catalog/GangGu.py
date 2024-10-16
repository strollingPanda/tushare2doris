# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():
    import api.GangGu.GangGuJiChuXinXi  # 港股_港股基础信息

    api.GangGu.GangGuJiChuXinXi.create_table()
    api.GangGu.GangGuJiChuXinXi.download()
    import api.GangGu.GangGuJiaoYiRiLi  # 港股_港股交易日历

    api.GangGu.GangGuJiaoYiRiLi.create_table()
    api.GangGu.GangGuJiaoYiRiLi.download()
    import api.GangGu.GangGuRiXianHangQing  # 港股_港股日线行情

    api.GangGu.GangGuRiXianHangQing.create_table()
    api.GangGu.GangGuRiXianHangQing.download()
    import api.GangGu.GangGuFuQuanHangQing  # 港股_港股复权行情

    api.GangGu.GangGuFuQuanHangQing.create_table()
    api.GangGu.GangGuFuQuanHangQing.download()


if __name__ == "__main__":

    download()
