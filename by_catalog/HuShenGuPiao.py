# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():

    # 沪深股票_基础数据_股票列表
    import api.HuShenGuPiao.JiChuShuJu.GuPiaoLieBiao

    api.HuShenGuPiao.JiChuShuJu.GuPiaoLieBiao.create_table()
    api.HuShenGuPiao.JiChuShuJu.GuPiaoLieBiao.download()

    # 沪深股票_基础数据_交易日历
    import api.HuShenGuPiao.JiChuShuJu.JiaoYiRiLi

    api.HuShenGuPiao.JiChuShuJu.JiaoYiRiLi.create_table()
    api.HuShenGuPiao.JiChuShuJu.JiaoYiRiLi.download()

    # 沪深股票_基础数据_沪深股通成份股
    import api.HuShenGuPiao.JiChuShuJu.HuShenGuTongChengFenGu

    api.HuShenGuPiao.JiChuShuJu.HuShenGuTongChengFenGu.create_table()
    api.HuShenGuPiao.JiChuShuJu.HuShenGuTongChengFenGu.download()

    # 沪深股票_基础数据_备用列表
    import api.HuShenGuPiao.JiChuShuJu.BeiYongLieBiao

    api.HuShenGuPiao.JiChuShuJu.BeiYongLieBiao.create_table()
    api.HuShenGuPiao.JiChuShuJu.BeiYongLieBiao.download()

    # 沪深股票_行情数据_日线行情
    import api.HuShenGuPiao.HangQingShuJu.RiXianHangQing

    api.HuShenGuPiao.HangQingShuJu.RiXianHangQing.create_table()
    api.HuShenGuPiao.HangQingShuJu.RiXianHangQing.download()

    # 沪深股票_行情数据_复权因子
    import api.HuShenGuPiao.HangQingShuJu.FuQuanYinZi

    api.HuShenGuPiao.HangQingShuJu.FuQuanYinZi.create_table()
    api.HuShenGuPiao.HangQingShuJu.FuQuanYinZi.download()

    # 沪深股票_行情数据_每日指标
    import api.HuShenGuPiao.HangQingShuJu.MeiRiZhiBiao

    api.HuShenGuPiao.HangQingShuJu.MeiRiZhiBiao.create_table()
    api.HuShenGuPiao.HangQingShuJu.MeiRiZhiBiao.download()

    # 沪深股票_行情数据_每日涨跌停价格
    import api.HuShenGuPiao.HangQingShuJu.MeiRiZhangDieTingJiaGe

    api.HuShenGuPiao.HangQingShuJu.MeiRiZhangDieTingJiaGe.create_table()
    api.HuShenGuPiao.HangQingShuJu.MeiRiZhangDieTingJiaGe.download()

    # 沪深股票_行情数据_每日停复牌信息
    import api.HuShenGuPiao.HangQingShuJu.MeiRiTingFuPaiXinXi

    api.HuShenGuPiao.HangQingShuJu.MeiRiTingFuPaiXinXi.create_table()
    api.HuShenGuPiao.HangQingShuJu.MeiRiTingFuPaiXinXi.download()

    # 沪深股票_行情数据_沪深股通十大成交股
    import api.HuShenGuPiao.HangQingShuJu.HuShenGuTongShiDaChengJiaoGu

    api.HuShenGuPiao.HangQingShuJu.HuShenGuTongShiDaChengJiaoGu.create_table()
    api.HuShenGuPiao.HangQingShuJu.HuShenGuTongShiDaChengJiaoGu.download()

    # 沪深股票_行情数据_港股通十大成交股
    import api.HuShenGuPiao.HangQingShuJu.GangGuTongShiDaChengJiaoGu

    api.HuShenGuPiao.HangQingShuJu.GangGuTongShiDaChengJiaoGu.create_table()
    api.HuShenGuPiao.HangQingShuJu.GangGuTongShiDaChengJiaoGu.download()

    # 沪深股票_行情数据_港股通每日成交统计
    import api.HuShenGuPiao.HangQingShuJu.GangGuTongMeiRiChengJiaoTongJi

    api.HuShenGuPiao.HangQingShuJu.GangGuTongMeiRiChengJiaoTongJi.create_table()
    api.HuShenGuPiao.HangQingShuJu.GangGuTongMeiRiChengJiaoTongJi.download()

    # 沪深股票_行情数据_备用行情
    import api.HuShenGuPiao.HangQingShuJu.BeiYongHangQing

    api.HuShenGuPiao.HangQingShuJu.BeiYongHangQing.create_table()
    api.HuShenGuPiao.HangQingShuJu.BeiYongHangQing.download()

    # 沪深股票_财务数据_利润表
    import api.HuShenGuPiao.CaiWuShuJu.LiRunBiao

    api.HuShenGuPiao.CaiWuShuJu.LiRunBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.LiRunBiao.download()

    # 沪深股票_财务数据_资产负债表
    import api.HuShenGuPiao.CaiWuShuJu.ZiChanFuZhaiBiao

    api.HuShenGuPiao.CaiWuShuJu.ZiChanFuZhaiBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.ZiChanFuZhaiBiao.download()

    # 沪深股票_财务数据_现金流量表
    import api.HuShenGuPiao.CaiWuShuJu.XianJinLiuLiangBiao

    api.HuShenGuPiao.CaiWuShuJu.XianJinLiuLiangBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.XianJinLiuLiangBiao.download()

    # 沪深股票_财务数据_业绩预告
    import api.HuShenGuPiao.CaiWuShuJu.YeJiYuGao

    api.HuShenGuPiao.CaiWuShuJu.YeJiYuGao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.YeJiYuGao.download()

    # 沪深股票_财务数据_业绩快报
    import api.HuShenGuPiao.CaiWuShuJu.YeJiKuaiBao

    api.HuShenGuPiao.CaiWuShuJu.YeJiKuaiBao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.YeJiKuaiBao.download()

    # 沪深股票_财务数据_分红送股数据
    import api.HuShenGuPiao.CaiWuShuJu.FenHongSongGuShuJu

    api.HuShenGuPiao.CaiWuShuJu.FenHongSongGuShuJu.create_table()
    api.HuShenGuPiao.CaiWuShuJu.FenHongSongGuShuJu.download()

    # 沪深股票_财务数据_财务指标数据
    import api.HuShenGuPiao.CaiWuShuJu.CaiWuZhiBiaoShuJu

    api.HuShenGuPiao.CaiWuShuJu.CaiWuZhiBiaoShuJu.create_table()
    api.HuShenGuPiao.CaiWuShuJu.CaiWuZhiBiaoShuJu.download()

    # 沪深股票_财务数据_财务审计意见
    import api.HuShenGuPiao.CaiWuShuJu.CaiWuShenJiYiJian

    api.HuShenGuPiao.CaiWuShuJu.CaiWuShenJiYiJian.create_table()
    api.HuShenGuPiao.CaiWuShuJu.CaiWuShenJiYiJian.download()

    # 沪深股票_财务数据_主营业务构成
    import api.HuShenGuPiao.CaiWuShuJu.ZhuYingYeWuGouCheng

    api.HuShenGuPiao.CaiWuShuJu.ZhuYingYeWuGouCheng.create_table()
    api.HuShenGuPiao.CaiWuShuJu.ZhuYingYeWuGouCheng.download()

    # 沪深股票_财务数据_财报披露日期表
    import api.HuShenGuPiao.CaiWuShuJu.CaiBaoPiLuRiQiBiao

    api.HuShenGuPiao.CaiWuShuJu.CaiBaoPiLuRiQiBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.CaiBaoPiLuRiQiBiao.download()

    # 沪深股票_两融及转融通_融资融券交易汇总
    import api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanJiaoYiHuiZong

    api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanJiaoYiHuiZong.create_table()
    api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanJiaoYiHuiZong.download()

    # 沪深股票_两融及转融通_融资融券交易明细
    import api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanJiaoYiMingXi

    api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanJiaoYiMingXi.create_table()
    api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanJiaoYiMingXi.download()

    # 沪深股票_两融及转融通_融资融券标的（盘前）
    import api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanBiaoDi_PanQian

    api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanBiaoDi_PanQian.create_table()
    api.HuShenGuPiao.LiangRongJiZhuanRongTong.RongZiRongQuanBiaoDi_PanQian.download()

    # 沪深股票_两融及转融通_转融券交易汇总
    import api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongQuanJiaoYiHuiZong

    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongQuanJiaoYiHuiZong.create_table()
    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongQuanJiaoYiHuiZong.download()

    # 沪深股票_两融及转融通_转融资交易汇总
    import api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongZiJiaoYiHuiZong

    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongZiJiaoYiHuiZong.create_table()
    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongZiJiaoYiHuiZong.download()

    # 沪深股票_两融及转融通_转融券交易明细
    import api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongQuanJiaoYiMingXi

    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongQuanJiaoYiMingXi.create_table()
    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZhuanRongQuanJiaoYiMingXi.download()

    # 沪深股票_两融及转融通_做市借券交易汇总
    import api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZuoShiJieQuanJiaoYiHuiZong

    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZuoShiJieQuanJiaoYiHuiZong.create_table()
    api.HuShenGuPiao.LiangRongJiZhuanRongTong.ZuoShiJieQuanJiaoYiHuiZong.download()

    # 沪深股票_资金流向数据_个股资金流向
    import api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang

    api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang.create_table()
    api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang.download()

    # 沪深股票_资金流向数据_个股资金流向（THS）
    import api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang_THS

    api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang_THS.create_table()
    api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang_THS.download()

    # 沪深股票_资金流向数据_个股资金流向（DC）
    import api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang_DC

    api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang_DC.create_table()
    api.HuShenGuPiao.ZiJinLiuXiangShuJu.GeGuZiJinLiuXiang_DC.download()

    # 沪深股票_资金流向数据_行业资金流向（THS）
    import api.HuShenGuPiao.ZiJinLiuXiangShuJu.HangYeZiJinLiuXiang_THS

    api.HuShenGuPiao.ZiJinLiuXiangShuJu.HangYeZiJinLiuXiang_THS.create_table()
    api.HuShenGuPiao.ZiJinLiuXiangShuJu.HangYeZiJinLiuXiang_THS.download()

    # 沪深股票_资金流向数据_板块资金流向（DC）
    import api.HuShenGuPiao.ZiJinLiuXiangShuJu.BanKuaiZiJinLiuXiang_DC

    api.HuShenGuPiao.ZiJinLiuXiangShuJu.BanKuaiZiJinLiuXiang_DC.create_table()
    api.HuShenGuPiao.ZiJinLiuXiangShuJu.BanKuaiZiJinLiuXiang_DC.download()

    # 沪深股票_资金流向数据_大盘资金流向（DC）
    import api.HuShenGuPiao.ZiJinLiuXiangShuJu.DaPanZiJinLiuXiang_DC

    api.HuShenGuPiao.ZiJinLiuXiangShuJu.DaPanZiJinLiuXiang_DC.create_table()
    api.HuShenGuPiao.ZiJinLiuXiangShuJu.DaPanZiJinLiuXiang_DC.download()

    # 沪深股票_资金流向数据_沪深港通资金流向
    import api.HuShenGuPiao.ZiJinLiuXiangShuJu.HuShenGangTongZiJinLiuXiang

    api.HuShenGuPiao.ZiJinLiuXiangShuJu.HuShenGangTongZiJinLiuXiang.create_table()
    api.HuShenGuPiao.ZiJinLiuXiangShuJu.HuShenGangTongZiJinLiuXiang.download()


if __name__ == "__main__":

    download()
