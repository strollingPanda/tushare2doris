# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():
    # 沪深股票_基础数据_股票列表(接口尚未实现)
    # 沪深股票_基础数据_每日股本（盘前）(接口尚未实现)

    import api.HuShenGuPiao.JiChuShuJu.JiaoYiRiLi  # 沪深股票_基础数据_交易日历

    api.HuShenGuPiao.JiChuShuJu.JiaoYiRiLi.create_table()
    api.HuShenGuPiao.JiChuShuJu.JiaoYiRiLi.download()
    # 沪深股票_基础数据_股票曾用名(接口尚未实现)
    # 沪深股票_基础数据_沪深股通成分股(接口尚未实现)
    # 沪深股票_基础数据_上市公司基本信息(接口尚未实现)
    # 沪深股票_基础数据_上市公司管理层(接口尚未实现)
    # 沪深股票_基础数据_管理层薪酬和持股(接口尚未实现)
    # 沪深股票_基础数据_IPO新股上市(接口尚未实现)
    # 沪深股票_基础数据_备用列表(接口尚未实现)

    import api.HuShenGuPiao.HangQingShuJu.RiXianHangQing  # 沪深股票_行情数据_日线行情

    api.HuShenGuPiao.HangQingShuJu.RiXianHangQing.create_table()
    api.HuShenGuPiao.HangQingShuJu.RiXianHangQing.download()

    # 沪深股票_行情数据_周线行情(接口尚未实现)
    # 沪深股票_行情数据_月线行情(接口尚未实现)
    # 沪深股票_行情数据_股票周/月线行情（每日更新）(接口尚未实现)
    # 沪深股票_行情数据_复权行情(接口尚未实现)

    import api.HuShenGuPiao.HangQingShuJu.FuQuanYinZi  # 沪深股票_行情数据_复权因子

    api.HuShenGuPiao.HangQingShuJu.FuQuanYinZi.create_table()
    api.HuShenGuPiao.HangQingShuJu.FuQuanYinZi.download()
    # 沪深股票_行情数据_实时快照（爬虫）(接口尚未实现)
    # 沪深股票_行情数据_实时成交（爬虫）(接口尚未实现)
    # 沪深股票_行情数据_实时排名（爬虫）(接口尚未实现)

    import api.HuShenGuPiao.HangQingShuJu.MeiRiZhiBiao  # 沪深股票_行情数据_每日指标

    api.HuShenGuPiao.HangQingShuJu.MeiRiZhiBiao.create_table()
    api.HuShenGuPiao.HangQingShuJu.MeiRiZhiBiao.download()
    # 沪深股票_行情数据_通用行情接口

    import api.HuShenGuPiao.HangQingShuJu.GeGuZiJinLiuXiang  # 沪深股票_行情数据_个股资金流向

    api.HuShenGuPiao.HangQingShuJu.GeGuZiJinLiuXiang.create_table()
    api.HuShenGuPiao.HangQingShuJu.GeGuZiJinLiuXiang.download()

    import api.HuShenGuPiao.HangQingShuJu.MeiRiZhangDieTingJiaGe  # 沪深股票_行情数据_每日涨跌停价格

    api.HuShenGuPiao.HangQingShuJu.MeiRiZhangDieTingJiaGe.create_table()
    api.HuShenGuPiao.HangQingShuJu.MeiRiZhangDieTingJiaGe.download()

    import api.HuShenGuPiao.HangQingShuJu.MeiRiTingFuPaiXinXi  # 沪深股票_行情数据_每日停复牌信息

    api.HuShenGuPiao.HangQingShuJu.MeiRiTingFuPaiXinXi.create_table()
    api.HuShenGuPiao.HangQingShuJu.MeiRiTingFuPaiXinXi.download()

    import api.HuShenGuPiao.HangQingShuJu.HuShenGuTongShiDaChengJiaoGu  # 沪深股票_行情数据_沪深股通十大成交股

    api.HuShenGuPiao.HangQingShuJu.HuShenGuTongShiDaChengJiaoGu.create_table()
    api.HuShenGuPiao.HangQingShuJu.HuShenGuTongShiDaChengJiaoGu.download()

    import api.HuShenGuPiao.HangQingShuJu.GangGuTongShiDaChengJiaoGu  # 沪深股票_行情数据_港股通十大成交股

    api.HuShenGuPiao.HangQingShuJu.GangGuTongShiDaChengJiaoGu.create_table()
    api.HuShenGuPiao.HangQingShuJu.GangGuTongShiDaChengJiaoGu.download()

    import api.HuShenGuPiao.HangQingShuJu.GangGuTongMeiRiChengJiaoTongJi  # 沪深股票_行情数据_港股通每日成交统计

    api.HuShenGuPiao.HangQingShuJu.GangGuTongMeiRiChengJiaoTongJi.create_table()
    api.HuShenGuPiao.HangQingShuJu.GangGuTongMeiRiChengJiaoTongJi.download()
    # 沪深股票_行情数据_港股通每月成交统计(接口尚未实现)

    import api.HuShenGuPiao.HangQingShuJu.BeiYongHangQing  # 沪深股票_行情数据_备用行情

    api.HuShenGuPiao.HangQingShuJu.BeiYongHangQing.create_table()
    api.HuShenGuPiao.HangQingShuJu.BeiYongHangQing.download()

    import api.HuShenGuPiao.CaiWuShuJu.LiRunBiao  # 沪深股票_财务数据_利润表

    api.HuShenGuPiao.CaiWuShuJu.LiRunBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.LiRunBiao.download()

    import api.HuShenGuPiao.CaiWuShuJu.ZiChanFuZhaiBiao  # 沪深股票_财务数据_资产负债表

    api.HuShenGuPiao.CaiWuShuJu.ZiChanFuZhaiBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.ZiChanFuZhaiBiao.download()

    import api.HuShenGuPiao.CaiWuShuJu.XianJinLiuLiangBiao  # 沪深股票_财务数据_现金流量表

    api.HuShenGuPiao.CaiWuShuJu.XianJinLiuLiangBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.XianJinLiuLiangBiao.download()

    import api.HuShenGuPiao.CaiWuShuJu.YeJiYuGao  # 沪深股票_财务数据_业绩预告

    api.HuShenGuPiao.CaiWuShuJu.YeJiYuGao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.YeJiYuGao.download()

    import api.HuShenGuPiao.CaiWuShuJu.YeJiKuaiBao  # 沪深股票_财务数据_业绩快报

    api.HuShenGuPiao.CaiWuShuJu.YeJiKuaiBao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.YeJiKuaiBao.download()

    import api.HuShenGuPiao.CaiWuShuJu.FenHongSongGuShuJu  # 沪深股票_财务数据_分红送股数据

    api.HuShenGuPiao.CaiWuShuJu.FenHongSongGuShuJu.create_table()
    api.HuShenGuPiao.CaiWuShuJu.FenHongSongGuShuJu.download()

    import api.HuShenGuPiao.CaiWuShuJu.CaiWuZhiBiaoShuJu  # 沪深股票_财务数据_财务指标数据

    api.HuShenGuPiao.CaiWuShuJu.CaiWuZhiBiaoShuJu.create_table()
    api.HuShenGuPiao.CaiWuShuJu.CaiWuZhiBiaoShuJu.download()

    import api.HuShenGuPiao.CaiWuShuJu.CaiWuShenJiYiJian  # 沪深股票_财务数据_财务审计意见

    api.HuShenGuPiao.CaiWuShuJu.CaiWuShenJiYiJian.create_table()
    api.HuShenGuPiao.CaiWuShuJu.CaiWuShenJiYiJian.download()

    import api.HuShenGuPiao.CaiWuShuJu.ZhuYingYeWuGouCheng  # 沪深股票_财务数据_主营业务构成

    api.HuShenGuPiao.CaiWuShuJu.ZhuYingYeWuGouCheng.create_table()
    api.HuShenGuPiao.CaiWuShuJu.ZhuYingYeWuGouCheng.download()

    import api.HuShenGuPiao.CaiWuShuJu.CaiBaoPiLuRiQiBiao  # 沪深股票_财务数据_财报披露日期表

    api.HuShenGuPiao.CaiWuShuJu.CaiBaoPiLuRiQiBiao.create_table()
    api.HuShenGuPiao.CaiWuShuJu.CaiBaoPiLuRiQiBiao.download()

    # 沪深股票_参考数据_前十大股东(接口尚未实现)
    # 沪深股票_参考数据_前十大流通股东(接口尚未实现)
    # 沪深股票_参考数据_龙虎榜每日明细(接口尚未实现)
    # 沪深股票_参考数据_龙虎榜机构交易明细(接口尚未实现)
    # 沪深股票_参考数据_股权质押统计数据(接口尚未实现)
    # 沪深股票_参考数据_股权质押明细数据(接口尚未实现)
    # 沪深股票_参考数据_股票回购(接口尚未实现)
    # 沪深股票_参考数据_概念股分类表(接口尚未实现)
    # 沪深股票_参考数据_概念股明细列表(接口尚未实现)
    # 沪深股票_参考数据_限售股解禁(接口尚未实现)
    # 沪深股票_参考数据_大宗交易(接口尚未实现)
    # 沪深股票_参考数据_股票开户数据（停）(接口尚未实现)
    # 沪深股票_参考数据_股票开户数据（旧）(接口尚未实现)
    # 沪深股票_参考数据_股东人数(接口尚未实现)
    # 沪深股票_参考数据_股东增减持(接口尚未实现)

    # 沪深股票_特色数据_券商盈利预测数据(接口尚未实现)
    # 沪深股票_特色数据_每日筹码及胜率(接口尚未实现)
    # 沪深股票_特色数据_每日筹码分布(接口尚未实现)
    # 沪深股票_特色数据_股票技术面因子(接口尚未实现)
    # 沪深股票_特色数据_股票技术面因子(专业版）(接口尚未实现)
    # 沪深股票_特色数据_中央结算系统持股统计(接口尚未实现)
    # 沪深股票_特色数据_中央结算系统持股明细(接口尚未实现)
    # 沪深股票_特色数据_沪深股通持股明细(接口尚未实现)
    # 沪深股票_特色数据_涨跌停和炸板数据(接口尚未实现)
    # 沪深股票_特色数据_机构调研数据(接口尚未实现)
    # 沪深股票_特色数据_券商月度金股(接口尚未实现)
    # 沪深股票_特色数据_游资名录(接口尚未实现)
    # 沪深股票_特色数据_游资每日明细(接口尚未实现)
    # 沪深股票_特色数据_同花顺App热榜(接口尚未实现)
    # 沪深股票_特色数据_东财App热榜(接口尚未实现)

    # 沪深股票_两融及转融通_融资融券交易汇总(接口尚未实现)
    # 沪深股票_两融及转融通_融资融券交易明细(接口尚未实现)
    # 沪深股票_两融及转融通_融资融券标的（盘前）(接口尚未实现)
    # 沪深股票_两融及转融通_转融券交易汇总(接口尚未实现)
    # 沪深股票_两融及转融通_转融资交易汇总(接口尚未实现)
    # 沪深股票_两融及转融通_转融券交易明细(接口尚未实现)
    # 沪深股票_两融及转融通_做市借券交易汇总(接口尚未实现)


if __name__ == "__main__":

    download()
