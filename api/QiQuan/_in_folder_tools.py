# -*- coding: utf-8 -*-
"""
加载整个项目根目录。创建目标数据库。
此文件不独立运行。
"""
import sys
import os

# 获得项目的根目录
abs_path_project = os.path.abspath(os.path.dirname(__file__))  # 先用此文件的绝对路径赋值
num = 2  # 需要去掉几个底层文件夹
for i in range(num):
    abs_path_project = os.path.split(abs_path_project)[0]
# 加载项目根目录
sys.path.append(abs_path_project)

import basis.basis_function

basis.basis_function.create_database()  # 创建数据库
