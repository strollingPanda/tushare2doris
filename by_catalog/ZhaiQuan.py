# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import _in_folder_tools


def download():

    # 债券_可转债基础信息
    import api.ZhaiQuan.KeZhuanZhaiJiChuXinXi

    api.ZhaiQuan.KeZhuanZhaiJiChuXinXi.create_table()
    api.ZhaiQuan.KeZhuanZhaiJiChuXinXi.download()

    # 债券_可转债发行
    import api.ZhaiQuan.KeZhuanZhaiFaXing

    api.ZhaiQuan.KeZhuanZhaiFaXing.create_table()
    api.ZhaiQuan.KeZhuanZhaiFaXing.download()

    # 债券_可转债赎回信息
    import api.ZhaiQuan.KeZhuanZhaiShuHuiXinXi

    api.ZhaiQuan.KeZhuanZhaiShuHuiXinXi.create_table()
    api.ZhaiQuan.KeZhuanZhaiShuHuiXinXi.download()

    # 债券_可转债票面利率
    import api.ZhaiQuan.KeZhuanZhaiPiaoMianLiLv

    api.ZhaiQuan.KeZhuanZhaiPiaoMianLiLv.create_table()
    api.ZhaiQuan.KeZhuanZhaiPiaoMianLiLv.download()

    # 债券_可转债行情
    import api.ZhaiQuan.KeZhuanZhaiHangQing

    api.ZhaiQuan.KeZhuanZhaiHangQing.create_table()
    api.ZhaiQuan.KeZhuanZhaiHangQing.download()

    # 债券_可转债转股结果
    import api.ZhaiQuan.KeZhuanZhaiZhuanGuJieGuo

    api.ZhaiQuan.KeZhuanZhaiZhuanGuJieGuo.create_table()
    api.ZhaiQuan.KeZhuanZhaiZhuanGuJieGuo.download()

    # 债券_债券回购日行情
    import api.ZhaiQuan.ZhaiQuanHuiGouRiHangQing

    api.ZhaiQuan.ZhaiQuanHuiGouRiHangQing.create_table()
    api.ZhaiQuan.ZhaiQuanHuiGouRiHangQing.download()

    # 债券_国债收益率曲线
    import api.ZhaiQuan.GuoZhaiShouYiLvQuXian

    api.ZhaiQuan.GuoZhaiShouYiLvQuXian.create_table()
    api.ZhaiQuan.GuoZhaiShouYiLvQuXian.download()


if __name__ == "__main__":

    download()
