# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():

    import api.ZhiShu.ZhiShuJiBenXinXi  # 指数_指数基本信息

    api.ZhiShu.ZhiShuJiBenXinXi.create_table()
    api.ZhiShu.ZhiShuJiBenXinXi.download()

    import api.ZhiShu.ZhiShuRiXianHangQing  # 指数_指数日线行情

    api.ZhiShu.ZhiShuRiXianHangQing.create_table()
    api.ZhiShu.ZhiShuRiXianHangQing.download()

    # 指数_指数周线行情(接口尚未实现)
    # 指数_指数月线行情(接口尚未实现)

    import api.ZhiShu.ZhiShuChengFenHeQuanZhong  # 指数_指数成分和权重

    api.ZhiShu.ZhiShuChengFenHeQuanZhong.create_table()
    api.ZhiShu.ZhiShuChengFenHeQuanZhong.download()

    import api.ZhiShu.DaPanZhiShuMeiRiZhiBiao  # 指数_大盘指数每日指标

    api.ZhiShu.DaPanZhiShuMeiRiZhiBiao.create_table()
    api.ZhiShu.DaPanZhiShuMeiRiZhiBiao.download()

    import api.ZhiShu.ShenWanHangYeFenLei  # 指数_申万行业分类

    api.ZhiShu.ShenWanHangYeFenLei.create_table()
    api.ZhiShu.ShenWanHangYeFenLei.download()

    import api.ZhiShu.ShenWanHangYeChengFen  # 指数_申万行业成分（分级）

    api.ZhiShu.ShenWanHangYeChengFen.create_table()
    api.ZhiShu.ShenWanHangYeChengFen.download()

    import api.ZhiShu.HuShenShiChangMeiRiJiaoYiTongJi  # 指数_沪深市场每日交易统计

    api.ZhiShu.HuShenShiChangMeiRiJiaoYiTongJi.create_table()
    api.ZhiShu.HuShenShiChangMeiRiJiaoYiTongJi.download()

    import api.ZhiShu.ShenZhenShiChangMeiRiJiaoYiQingKuang  # 指数_深圳市场每日交易情况

    api.ZhiShu.ShenZhenShiChangMeiRiJiaoYiQingKuang.create_table()
    api.ZhiShu.ShenZhenShiChangMeiRiJiaoYiQingKuang.download()

    import api.ZhiShu.TongHuaShunGaiNianHeHangYeLieBiao  # 指数_同花顺概念和行业列表

    api.ZhiShu.TongHuaShunGaiNianHeHangYeLieBiao.create_table()
    api.ZhiShu.TongHuaShunGaiNianHeHangYeLieBiao.download()

    import api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuHangQing  # 指数_同花顺概念和行业指数行情

    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuHangQing.create_table()
    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuHangQing.download()

    import api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuChengFen  # 指数_同花顺概念和行业指数成分

    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuChengFen.create_table()
    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuChengFen.download()

    import api.ZhiShu.ZhongXinHangYeZhiShuRiHangQing  # 指数_中信行业指数日行情

    api.ZhiShu.ZhongXinHangYeZhiShuRiHangQing.create_table()
    api.ZhiShu.ZhongXinHangYeZhiShuRiHangQing.download()

    import api.ZhiShu.ShenWanHangYeZhiShuRiHangQing  # 指数_申万行业指数日行情

    api.ZhiShu.ShenWanHangYeZhiShuRiHangQing.create_table()
    api.ZhiShu.ShenWanHangYeZhiShuRiHangQing.download()

    # 指数_国际主要指数(接口尚未实现)


if __name__ == "__main__":

    download()
