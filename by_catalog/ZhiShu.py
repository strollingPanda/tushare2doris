# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():
    # 指数_指数基本信息
    import api.ZhiShu.ZhiShuJiBenXinXi

    api.ZhiShu.ZhiShuJiBenXinXi.create_table()
    api.ZhiShu.ZhiShuJiBenXinXi.download()

    # 指数_指数日线行情
    import api.ZhiShu.ZhiShuRiXianHangQing

    api.ZhiShu.ZhiShuRiXianHangQing.create_table()
    api.ZhiShu.ZhiShuRiXianHangQing.download()

    # 指数_指数成分和权重
    import api.ZhiShu.ZhiShuChengFenHeQuanZhong

    api.ZhiShu.ZhiShuChengFenHeQuanZhong.create_table()
    api.ZhiShu.ZhiShuChengFenHeQuanZhong.download()

    # 指数_大盘指数每日指标
    import api.ZhiShu.DaPanZhiShuMeiRiZhiBiao

    api.ZhiShu.DaPanZhiShuMeiRiZhiBiao.create_table()
    api.ZhiShu.DaPanZhiShuMeiRiZhiBiao.download()

    # 指数_申万行业分类
    import api.ZhiShu.ShenWanHangYeFenLei

    api.ZhiShu.ShenWanHangYeFenLei.create_table()
    api.ZhiShu.ShenWanHangYeFenLei.download()

    # 指数_申万行业成分（分级）
    import api.ZhiShu.ShenWanHangYeChengFen

    api.ZhiShu.ShenWanHangYeChengFen.create_table()
    api.ZhiShu.ShenWanHangYeChengFen.download()

    # 指数_沪深市场每日交易统计
    import api.ZhiShu.HuShenShiChangMeiRiJiaoYiTongJi

    api.ZhiShu.HuShenShiChangMeiRiJiaoYiTongJi.create_table()
    api.ZhiShu.HuShenShiChangMeiRiJiaoYiTongJi.download()

    # 指数_深圳市场每日交易情况
    import api.ZhiShu.ShenZhenShiChangMeiRiJiaoYiQingKuang

    api.ZhiShu.ShenZhenShiChangMeiRiJiaoYiQingKuang.create_table()
    api.ZhiShu.ShenZhenShiChangMeiRiJiaoYiQingKuang.download()

    # 指数_同花顺概念和行业列表
    import api.ZhiShu.TongHuaShunGaiNianHeHangYeLieBiao

    api.ZhiShu.TongHuaShunGaiNianHeHangYeLieBiao.create_table()
    api.ZhiShu.TongHuaShunGaiNianHeHangYeLieBiao.download()

    # 指数_同花顺概念和行业指数行情
    import api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuHangQing

    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuHangQing.create_table()
    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuHangQing.download()

    # 指数_同花顺概念和行业指数成分
    import api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuChengFen

    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuChengFen.create_table()
    api.ZhiShu.TongHuaShunGaiNianHeHangYeZhiShuChengFen.download()

    # 指数_中信行业指数日行情
    import api.ZhiShu.ZhongXinHangYeZhiShuRiHangQing

    api.ZhiShu.ZhongXinHangYeZhiShuRiHangQing.create_table()
    api.ZhiShu.ZhongXinHangYeZhiShuRiHangQing.download()

    # 指数_申万行业指数日行情
    import api.ZhiShu.ShenWanHangYeZhiShuRiHangQing

    api.ZhiShu.ShenWanHangYeZhiShuRiHangQing.create_table()
    api.ZhiShu.ShenWanHangYeZhiShuRiHangQing.download()


if __name__ == "__main__":

    download()
