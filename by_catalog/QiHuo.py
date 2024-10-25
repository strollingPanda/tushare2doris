# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():
    # 期货_合约信息
    import api.QiHuo.HeYueXinXi

    api.QiHuo.HeYueXinXi.create_table()
    api.QiHuo.HeYueXinXi.download()

    # 期货_交易日历
    import api.QiHuo.JiaoYiRiLi

    api.QiHuo.JiaoYiRiLi.create_table()
    api.QiHuo.JiaoYiRiLi.download()

    # 期货_日线行情
    import api.QiHuo.RiXianHangQing

    api.QiHuo.RiXianHangQing.create_table()
    api.QiHuo.RiXianHangQing.download()

    # 期货_每日结算参数
    import api.QiHuo.MeiRiJieSuanCanShu

    api.QiHuo.MeiRiJieSuanCanShu.create_table()
    api.QiHuo.MeiRiJieSuanCanShu.download()

    # 期货_南华期货指数行情
    import api.QiHuo.NanHuaQiHuoZhiShuHangQing

    api.QiHuo.NanHuaQiHuoZhiShuHangQing.create_table()
    api.QiHuo.NanHuaQiHuoZhiShuHangQing.download()

    # 期货_期货主力与连续合约
    import api.QiHuo.QiHuoZhuLiYuLianXuHeYue

    api.QiHuo.QiHuoZhuLiYuLianXuHeYue.create_table()
    api.QiHuo.QiHuoZhuLiYuLianXuHeYue.download()


if __name__ == "__main__":

    download()
