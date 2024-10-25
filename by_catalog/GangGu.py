# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():
    # 港股_港股基础信息
    import api.GangGu.GangGuJiChuXinXi

    api.GangGu.GangGuJiChuXinXi.create_table()
    api.GangGu.GangGuJiChuXinXi.download()

    # 港股_港股交易日历
    import api.GangGu.GangGuJiaoYiRiLi

    api.GangGu.GangGuJiaoYiRiLi.create_table()
    api.GangGu.GangGuJiaoYiRiLi.download()

    # 港股_港股日线行情
    import api.GangGu.GangGuRiXianHangQing

    api.GangGu.GangGuRiXianHangQing.create_table()
    api.GangGu.GangGuRiXianHangQing.download()

    # 港股_港股复权行情
    import api.GangGu.GangGuFuQuanHangQing

    api.GangGu.GangGuFuQuanHangQing.create_table()
    api.GangGu.GangGuFuQuanHangQing.download()


if __name__ == "__main__":

    download()
