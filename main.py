#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
import basis.basis_function
import by_catalog.HuShenGuPiao
import by_catalog.ZhiShu
import by_catalog.QiHuo
import by_catalog.GangGu

sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # 加载项目根目录


basis.basis_function.create_database()  # 创建数据库


# 执行下载
by_catalog.HuShenGuPiao.download()  # 沪深股票
by_catalog.ZhiShu.download()  # 指数
by_catalog.QiHuo.download()  # 期货
by_catalog.GangGu.download()  # 港股
print(1)
