# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():
    # 期权_期权合约信息
    import api.QiQuan.QiQuanHeYueXinXi

    api.QiQuan.QiQuanHeYueXinXi.create_table()
    api.QiQuan.QiQuanHeYueXinXi.download()

    # 期权_期权日线行情
    import api.QiQuan.QiQuanRiXianHangQing

    api.QiQuan.QiQuanRiXianHangQing.create_table()
    api.QiQuan.QiQuanRiXianHangQing.download()


if __name__ == "__main__":

    download()
