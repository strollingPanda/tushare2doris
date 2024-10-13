# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():
    import api.QiHuo.HeYueXinXi  # 期货_合约信息

    api.QiHuo.HeYueXinXi.create_table()
    api.QiHuo.HeYueXinXi.download()

    import api.QiHuo.JiaoYiRiLi  # 期货_交易日历

    api.QiHuo.JiaoYiRiLi.create_table()
    api.QiHuo.JiaoYiRiLi.download()

    import api.QiHuo.RiXianHangQing  # 期货_日线行情

    api.QiHuo.RiXianHangQing.create_table()
    api.QiHuo.RiXianHangQing.download()
    # 期货_期货周/月线行情（每日更新）(接口尚未实现)
    # 期货_分钟行情(接口尚未实现)
    # 期货_Tick行情(接口尚未实现)
    # 期货_仓单日报(接口尚未实现)

    import api.QiHuo.MeiRiJieSuanCanShu  # 期货_每日结算参数

    api.QiHuo.MeiRiJieSuanCanShu.create_table()
    api.QiHuo.MeiRiJieSuanCanShu.download()

    # 期货_每日持仓排名(接口尚未实现)

    import api.QiHuo.NanHuaQiHuoZhiShuHangQing  # 期货_南华期货指数行情

    api.QiHuo.NanHuaQiHuoZhiShuHangQing.create_table()
    api.QiHuo.NanHuaQiHuoZhiShuHangQing.download()

    import api.QiHuo.QiHuoZhuLiYuLianXuHeYue  # 期货_期货主力与连续合约

    api.QiHuo.QiHuoZhuLiYuLianXuHeYue.create_table()
    api.QiHuo.QiHuoZhuLiYuLianXuHeYue.download()
    # 期货_期货主要品种交易周报(接口尚未实现)


if __name__ == "__main__":

    download()
