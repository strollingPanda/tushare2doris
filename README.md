- [tushare2doris简介](#tushare2doris简介)
- [开源程序的目的](#开源程序的目的)
- [程序结构和使用](#程序结构和使用)
    - [程序主要文件夹和文件](#程序主要文件夹和文件)
    - [程序运行需要的环境](#程序运行需要的环境)
    - [如何运行本程序](#如何运行本程序)
    - [tushare积分对运行本程序的影响](#tushare积分对运行本程序的影响)
- [已实现和未实现接口](#已实现和未实现接口)
- [在Ubuntu 24中安装Doris 3.0.1](#在ubuntu-24中安装doris-301)
    - [安装前的环境装备](#安装前的环境装备)
        - [修改系统能打开的文件的最大数量](#修改系统能打开的文件的最大数量)
        - [disable the swap partition](#disable-the-swap-partition)
        - [Install Build Tools](#install-build-tools)
        - [安装java环境](#安装java环境)
    - [配置FE](#配置fe)
        - [查看CPU是否支持AVX2](#查看cpu是否支持avx2)
        - [如果CPU不支持avx2](#如果cpu不支持avx2)
        - [如果CPU支持avx2](#如果cpu支持avx2)
        - [修改FE配置文件](#修改fe配置文件)
        - [防火墙开启端口](#防火墙开启端口)
        - [查看FE是否运行成功](#查看fe是否运行成功)
    - [安装mysql client](#安装mysql-client)
    - [配置BE](#配置be)
        - [安装JAVE环境](#安装jave环境)
        - [安装JAVA UDF](#安装java-udf)
        - [防火墙开启端口](#防火墙开启端口-1)
        - [启动BE](#启动be)
        - [添加BE节点到集群](#添加be节点到集群)
        - [在mysql中查看BE状态。](#在mysql中查看be状态)
    - [重启电脑后启动FE、BE](#重启电脑后启动febe)

# tushare2doris简介
本程序用于将tushare数据同步至本地Doris数据库。
# 开源程序的目的
引用[WonderTrader](https://github.com/wondertrader/wondertrader)作者雁过也的[一段文字](https://zhuanlan.zhihu.com/p/341240866)：  


"...笔者希望WonderTrader能够被更多的人认可和使用，但是笔者毕竟精力有限，没办法像专业的平台公司一样，投入大量的人力物力做推广，更没办法7×24小时做技术支持，所以开源就成为了一个很好的方案。开源以后，一方面WonderTrader可以有机会给更多的人使用，吸收更多的需求不断地完善；另一方面笔者也有机会认识更多的同路人，对笔者来说也是一个可以学习更多知识的切入点。..."  

更好地维护软件、和更优秀的同路人交流，这也是tushare2doris开源的主要目的。  

另外一件事情，我和几位网络相识的小伙伴，正在尝试开发另外一套开源程序，进行股指期货日内策略的回测和交易。程序会包含历史数据整理、策略回测、交易等几个部分。  

我们可以提供还不错的数据和硬件支持，相关工作现在也有了一点基础。  

这个工作正在进行中，有兴趣参加相关开发工作的伙伴，可以直接加我微信 cm303287345。  

# 程序结构和使用
### 程序主要文件夹和文件

| 文件夹或文件名称      | 描述 |
| ----------- | ----------- |
|  ./api/     | 文件夹内每一个py文件是对tushare每一个接口的具体实现。 比如 ./api/HuShenGuPiao/HangQingShuJu/RiXianHangQing.py为对tushare的 沪深股票--行情数据--日线行情 接口的实现。这些用于实现tushare接口的py文件可以独立运行。      |
|  ./basis/  |  该文件夹提供了程序运行需要的一些基础工具。其中 ./basis/config.yaml文件中存放了不同接口下载过程中需要的控制参数。      |
|  ./by_catalog/  |文件夹内的py文件与tushare的一级目录一一对应，用于调用该一级目录下的所有接口。比如./by_catalog/HuShenGuPiao.py调用了tushare沪深股票目录下所有已实现的接口。该文件夹下的py文件可以独立运行。    |
|  ./log/  |    该文件夹在程序首次运行后自动生成，用于存储程序运行的log文件。     |
|  ./pydori/  | 该文件夹内容为python包pydoris-client 1.0.4的源码，本程序对其中./pydoris/doris_client.py的write函数进行了改写。        |
|  ./main.py  | 该文件用于同步tushare所有已实现的接口。      |
  
### 程序运行需要的环境
本软件运行需要的python包有tushare、 yaml、 datetime、 loguru、 mysql。 

以下展示在Ubuntu 24上安装虚拟环境，并安装python包。    

以root身份登陆terminal，运行以下代码以安装venv 
`apt install python3.12-venv`  

在terminal上，运行下面代码，以进入/alpha-suite/tushare2doris文件夹，生成本程序需要的虚拟环境  
`cd /alpha-suite/tushare2doris/`  
`python3 -m venv venv-tushare2doris`  

运行下面代码以启用虚拟环境    
`source venv-tushare2doris/bin/activate`  

在虚拟环境下，运行如下代码，安装所需的python包  

    pip install tushare
    pip install pyyaml
    pip install datetime
    pip install loguru
    pip install mysql-connector-python
### 如何运行本程序
+ 如果需要同步所有已实现接口的数据，可以直接运行./main.py文件。  
+ 如果想同步tushare某一级目录下所有已实现接口的数据，可以运行./by_catalog/文件夹下的具体py文件。比如，若只需同步tushare一级目录沪深股票下的接口，可以单独运行./by_catalog/HuShenGuPiao.py。  
+ 如果想同步tushare某一个接口的数据，可以运行./api/目录下的py文件。比如，若想同步沪深股票--行情数据--日线行情，可以运行./api/HuShenGuPiao/HangQingShuJu/RiXianHangQing.py。但此处需特别留意不同接口之间的依赖关系，比如我们在实现沪深股票--行情数据--日线行情这个接口的过程中，调用了沪深股票--基础数据--交易日历的数据，因此，为了保证数据完整性，需先运行文件./api/HuShenGuPiao/JiChuShuJu/JiaoYiRiLi.py。  

### tushare积分对运行本程序的影响
如果使用者tushare积分大于等于5000，同时有港股日线权限，可以按默认设置执行本程序。    

如果使用者积分低于5000，需要做两处更改：  
1. 部分接口将不再具有下载权限，因此使用中需要将这些接口屏蔽掉。[tushare权限说明](https://tushare.pro/document/1?doc_id=108)
2. 从tushare下载数据频次需要降低，可以通过修改./basis/config.yaml文件中regular_gap的值实现。[tushare积分与频次权限对应表](https://tushare.pro/document/1?doc_id=290)  

如果使用者无tushare港股日线权限，请将相关接口屏蔽。
# 已实现和未实现接口
在本小节以下内容中，[此样式字体]() 表示该接口已实现，正常字体表示该接口未实现。（以下目录在20241015日确认与tushare网站目录一致）
+ 沪深股票
  + 基础数据
    + [股票列表]()
    + 每日股本（盘前）
    + [交易日历]()
    + 股票曾用名
    + [沪深股通成分股]()
    + 上市公司基本信息
    + 上市公司管理层
    + 管理层薪酬和持股
    + IPO新股上市
    + [备用列表]()
  + 行情数据
    + [日线行情]()
    + 周线行情
    + 月线行情
    + 股票周/月线行情(每日更新)
    + 复权行情
    + [复权因子]()
    + 实时快照（爬虫）
    + 实时成交（爬虫）
    + 实时排名（爬虫）
    + [每日指标]()
    + 通用行情接口
    + [每日涨跌停价格]()
    + [每日停复牌信息]()
    + [沪深股通十大成交股]()
    + [港股通十大成交股]()
    + [港股通每日成交统计]()
    + 港股通每月成交统计
    + [备用行情]()
  + 财务数据
    + [利润表]()
    + [资产负债表]()
    + [现金流量表]()
    + [业绩预告]()
    + [业绩快报]()
    + [分红送股数据]()
    + [财务指标数据]()
    + [财务审计意见]()
    + [主营业务构成]()
    + [财报披露日期表]()
  + 参考数据
    + 前十大股东
    + 前十大流通股东
    + 龙虎榜每日明细
    + 龙虎榜机构交易明细
    + 股权质押统计数据
    + 股权质押明细数据
    + 股票回购
    + 概念股分类表
    + 概念股明细列表
    + 限售股解禁
    + 大宗交易
    + 股票开户数据（停）
    + 股票开户数据（旧）
    + 股东人数
    + 股东增减持
  + 特色数据
    + 券商盈利预测数据
    + 每日筹码及胜率
    + 每日筹码分布
    + 股票技术面因子
    + 股票技术面因子(专业版）
    + 中央结算系统持股统计
    + 中央结算系统持股明细
    + 沪深股通持股明细
    + 机构调研数据
    + 券商月度金股
  + 两融及转融通
    + [个股资金流向融资融券交易汇总]()
    + [个股资金流向融资融券交易明细]()
    + [个股资金流向融资融券标的（盘前）]()
    + [个股资金流向转融券交易汇总]()
    + [个股资金流向转融资交易汇总]()
    + [个股资金流向转融券交易明细]()
    + [个股资金流向做市借券交易汇总]()
  + 资金流向数据
    + [个股资金流向]()
    + [个股资金流向（THS）]()
    + [个股资金流向（DC）]()
    + [行业资金流向（THS）]()
    + [板块资金流向（DC）]()
    + [大盘资金流向（DC）]()
    + [沪深港通资金流向]()
  + 打板专题数据
    + 题材数据（开盘啦）
    + 题材成分（开盘啦）
    + 榜单数据（开盘啦）
    + 涨跌停和炸板数据
    + 游资名录
    + 游资每日明细
    + 同花顺App热榜
    + 东方财富App热榜
+ 指数
  + [指数基本信息]()
  + [指数日线行情]()
  + 指数周线行情
  + 指数月线行情
  + [指数成分和权重]()
  + [大盘指数每日指标]()
  + [申万行业分类]()
  + [申万行业成分（分级）]()
  + [沪深市场每日交易统计]()
  + [深圳市场每日交易情况]()
  + [同花顺概念和行业列表]()
  + [同花顺概念和行业指数行情]()
  + [同花顺概念和行业指数成分]()
  + [中信行业指数日行情]()
  + [申万行业指数日行情]()
  + 国际主要指数
+ 公募基金
  + 基金列表
  + 基金管理人
  + 基金经理
  + 基金规模
  + 基金净值
  + 基金分红
  + 基金持仓
  + 基金行情（含ETF）
  + 复权因子
+ 期货
  + [合约信息]()
  + [交易日历]()
  + [日线行情]()
  + 期货周/月线行情(每日更新)
  + 历史分钟行情
  + 实时分钟行情
  + 仓单日报
  + [每日结算参数]()
  + 历史Tick行情
  + 每日持仓排名
  + [南华期货指数行情]()
  + [期货主力与连续合约]()
  + 期货主要品种交易周报
+ 现货
  + 上海黄金基础信息
  + 上海黄金现货日行情
+ 期权
  + [期权合约信息]()
  + [期权日线行情]()
  + 期权分钟行情
+ 债券
  + [可转债基础信息]()
  + [可转债发行]()
  + [可转债赎回信息]()
  + [可转债票面利率]()
  + [可转债行情]()
  + 可转债转股价变动
  + [可转债转股结果]()
  + [债券回购日行情]()
  + 柜台流通式债券报价
  + 柜台流通式债券最优报价
  + 大宗交易
  + 大宗交易明细
  + [国债收益率曲线]()
  + 全球财经事件
+ 外汇
  + 外汇基础信息（海外）
  + 外汇日线行情
+ 港股
  + [港股基础信息]()
  + [港股交易日历]()
  + [港股日线行情]()
  + [港股复权行情]()
  + 港股分钟行情
+ 美股
  + 美股基础信息
  + 美股交易日历
  + 美股日线行情
  + 美股复权行情
+ 行业经济
  + TMT行业
    + 台湾电子产业月营收
    + 台湾电子产业月营收明细
    + 电影月度票房
    + 电影周度票房
    + 电影日度票房
    + 影院日度票房
    + 全国电影剧本备案数据
    + 全国电视剧备案公示数据
+ 宏观经济
  + 国内宏观
    + 利率数据
      + Shibor利率
      + Shibor报价数据
      + LPR贷款基础利率
      + Libor利率
      + Hibor利率
      + 温州民间借贷利率
      + 广州民间借贷利率
    + 国民经济
      + 国内生产总值（GDP）
    + 价格指数
      + 居民消费价格指数（CPI）
      + 工业生产者出厂价格指数（PPI）
    + 金融
      + 货币供应量
        + 货币供应量（月）
      + 社会融资
        + 社融增量（月度）
    + 景气度
      + 采购经理指数（PMI）
  + 国际宏观
    + 美国利率
      + 国债收益率曲线利率
      + 国债实际收益率曲线利率
      + 短期国债利率
      + 国债长期利率
      + 国债长期利率平均值
+ 另类数据
  + 新闻快讯
  + 新闻通讯（长篇）
  + 新闻联播文字稿
  + 上市公司公告
  + 新冠状肺炎感染人数
  + 全球新冠疫情数据
+ 财富管理
  + 基金销售行业数据
  + 各渠道公募基金销售保有规模占比
  + 销售机构公募基金销售保有规模

# 在Ubuntu 24中安装Doris 3.0.1 
Doris的架构非常简单，一般只有两个进程：Frontend（FE）和Backend（BE），这两个进程都可以横向扩展至数百台机器。其中FE负责用户请求的接入、查询解析规划、元数据的存储、节点管理相关工作；BE主要负责数据存储、查询计划的执行。

在以下的演示中，FE和BE安装在同一台机器上。

如果安装操作系统不同，或安装的Doris版本不同，安装步骤会略有区别。比如Doris 3.0.1需要的JAVA环境是17，而Doris 2.1.6需要的JAVA环境是8（或称1.8）。

以下演示在Ubuntu 24.04的/alpha-suite/doris/文件夹中安装Doris 3.0.1。 

### 安装前的环境装备
以root用户登陆Ubuntu。

##### 修改系统能打开的文件的最大数量

在terminal里运行`ulimit -n`，查询available limit

在terminal里运行`vim /etc/sysctl.conf`，在打开的文件中添加如下代码  
`fs.file-max = 65535`

在teriminal里运行`sysctl -p`,以更新设置

如果系统提示 sysctl: permission denied on key ‘fs.file-max’, 可以直接跳过该提示。

在terminal中运行`vim /etc/security/limits.conf`, 在打开的文件中增加如下代码

    * soft     nproc          65535    
    * hard     nproc          65535   
    * soft     nofile         65535   
    * hard     nofile         65535
    root soft     nproc          65535    
    root hard     nproc          65535   
    root soft     nofile         65535   
    root hard     nofile         65535
在terimnal中运行`vim /etc/pam.d/common-session`，增加如下代码
`session required pam_limits.so`

在terminal中运行`logout`，并重新login。在termail中运行`ulimit -n`,应返回65535。

##### disable the swap partition  
在terminal中运行  
`swapoff -a`

##### Install Build Tools
在terminal中运行如下代码  
`apt update`  
`apt install build-essential maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build`

##### 安装java环境  
创建并进入目标文件夹，  
`mkdir -p /alpah-suite/doris/`  
`cd /alpah-suite/doris/`  
在terminal中运行如下代码，下载jdk  
`wget https://download.oracle.com/java/17/archive/jdk-17.0.11_linux-x64_bin.tar.gz`  
解压  
`tar -zxf jdk-17.0.11_linux-x64_bin.tar.gz`  
设置JAVA_HOME，在termianl中运行  
`vim /etc/profile`  
在打开的文件中增加以下内容 

    export JAVA_HOME=/alpha-suite/doris/jdk-17.0.11
    export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
    export PATH=$JAVA_HOME/bin:$PATH
在termianl中运行如下代码，以重新加载  
`source /etc/profile`  
在termianl中运行如下代码，以检查java环境  
`java -version`  
此时应显示类似以下内容

    java version "17.0.11" 2024-04-16 LTS
    Java(TM) SE Runtime Environment (build 17.0.11+7-LTS-207)
    Java HotSpot(TM) 64-Bit Server VM (build 17.0.11+7-LTS-207, mixed mode, sharing)

### 配置FE
##### 查看CPU是否支持AVX2
在terminal中运行下面代码，以查看cpu是否支持AVX2  
`grep avx2 /proc/cpuinfo`  
如果看到类似下面的输出，说明你的 CPU 支持 AVX2 指令集。  
flags                : ... avx2 ...  
如果看不到任何输出，说明CPU不持支。
根据以上结果，选择下载doris的版本。

##### 如果CPU不支持avx2
在terminal运行如下代码，以下载Doris  
`cd /alpha-suite/doris`  
`wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-3.0.1-bin-x64-noavx2.tar.gz`  
解压缩  
`tar -zxf apache-doris-3.0.1-bin-x64-noavx2.tar.gz`  
拷贝doris文件至上一层目录  
`cp -r /alpha-suite/doris/apache-doris-3.0.1-bin-x64-noavx2/* /alpha-suite/doris`

##### 如果CPU支持avx2  
在terminal运行如下代码，以下载Doris  
`cd /alpha-suite/doris`  
`wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-3.0.1-bin-x64.tar.gz`  
解压缩  
`tar -zxf apache-doris-3.0.1-bin-x64.tar.gz`  
拷贝doris文件至上一层目录  
`cp -r /alpha-suite/doris/apache-doris-3.0.1-bin-x64/* /alpha-suite/doris`

##### 修改FE配置文件 
在terminal中运行如下代码，以修改 FE 配置文件    
`vim /alpha-suite/doris/fe/conf/fe.conf`  
在打开的文件中增加下内容，  

    meta_dir = /alpha-suite/doris/fe/doris-meta  
    priority_networks=x.x.x.x/24  
    table_name_length_limit = 200  
并修改  
`arrow_flight_sql_port = 9090`  
其中，meta_dir 为元数据目录，需要你提前创建好你指定的目录。生产环境强烈建议单独指定目录，不要放在Doris安装目录下，最好是单独的磁盘，如果有SSD最好。
priority_networks后面的"x.x.x.x"为ip。在我们演示的中，我们只在一台机器上安装FE，此处可以设置ip为127.0.0.1  
table_name_length_limit为允许的表名称长度

##### 防火墙开启端口
在terminal中运行如下代码，让防火墙开启这些端口

    ufw allow 8030/tcp
    ufw allow 9020/tcp
    ufw allow 9030/tcp
    ufw allow 9010/tcp
在terminal中运行如下代码来启动FE  
`bash /alpha-suite/doris/fe/bin/start_fe.sh --daemon`

##### 查看FE是否运行成功
在terminal中运行如下代码，查看FE是否运行成功  
curl http://127.0.0.1:8030/api/bootstrap  
如果运行成功，会返回如下字段

    {"msg":"success","code":0,"data":{"replayedJournalId":0,"queryPort":0,"rpcPort":0,"arrowFlightSqlPort":0,"version":""},"count":0}
运行成功后，可以通过浏览器登陆doris，地址为http://x.x.x.x:8030，可以以用Doris内置的默认用户root进行登录，密码是空。这是一个地址展示的是Doris的管理界面，只能拥有管理权限的用户才能登录，普通用户不能登录。

### 安装mysql client
在terminal中运行如下命令，安装mysql  
`apt-get install mysql-client`  
在terminal中运行如下命令，查看mysql是否安装成功  
`mysql --version`  
在terimnal中运行如下命令，用mysql client登陆。  
`mysql -uroot -P9030 -h127.0.0.1`  
这里使用的root用户是doris内置的默认用户，也是超级管理员用户。    
-P ：这里是我们连接 Doris 的查询端口，默认端口是9030，对应的是fe.conf里的 query_port  
-h ：这里是我们连接的 FE IP地址，如果客户端和FE安装在同一个节点可以使用127.0.0.1。

用mysql连接Doris成功后，在mysql运行如下命令，设置root账户的登录密码为1234    
`SET PASSWORD FOR 'root' = PASSWORD('1234');`  

在mysql中执行下面的命令，查看 FE 运行状态。  
`show frontends\G;`  
如果 IsMaster、Join 和 Alive 三列均为true，则表示节点正常。

### 配置BE
在terminal中运行如下命令，创建两个用作存储的文件夹  
`mkdir -p /alpha-suite/doris/disk1/doris`  
`mkdir -p /alpha-suite/doris/disk2/doris`  
在terminal中运行如下命令，打开BE的配置文件  
`vim /alpha-suite/doris/be/conf/be.conf`  
在打开的文件中，增加如下代码  

    priority_networks=x.x.x.x/24  
    storage_root_path=/alpha-suite/doris/disk1/doris,medium:HDD;/alpha-suite/doris/disk2/doris,medium:SSD
同时做如下修改  
`arrow_flight_sql_port = 9091`  
其中x.x.x.x为ip地址，我们可以设置为127.0.0.1。  
storage_root_path为数据存放目录，默认在be/storage下，若需要指定目录的话，需要预创建目录。多个路径之间使用英文状态的分号 ; 分隔。可以通过路径区别节点内的冷热数据存储目录，HDD（冷数据目录）或 SSD（热数据目录）。如果不需要 BE 节点内的冷热机制，那么只需要配置路径即可，无需指定 medium 类型；也不需要修改FE的默认存储介质配置。如果未指定存储路径的存储类型，则默认全部为 HDD（冷数据目录）。  
这里的 HDD 和 SSD 与物理存储介质无关，只为了区分存储路径的存储类型，即可以在 HDD 介质的盘上标记某个目录为 SSD（热数据目录）。  
示例1不区分medium，代码如下：  
`storage_root_path=/home/disk1/doris;/home/disk2/doris;/home/disk2/doris`  
示例2使用 storage_root_path 参数里指定 medium，代码如下：  
`storage_root_path=/home/disk1/doris,medium:HDD;/home/disk2/doris,medium:SSD`  
其中  
/home/disk1/doris,medium:HDD： 表示该目录存储冷数据;  
/home/disk2/doris,medium:SSD： 表示该目录存储热数据;

##### 安装JAVE环境
如在一个单独的机器上配置BE，需重新配置JAVA_HOME环境变量。此处因在同一台机器上安装，java环境之前已经配置过。

##### 安装JAVA UDF 
在terminal运行如下命令  
`cd /alpha-suite/doris/be/lib`  
`cp java_extensions/java-udf/java-udf-jar-with-dependencies.jar .`

##### 防火墙开启端口
在terminal运行如下命令

    ufw allow 9060/tcp
    ufw allow 8040/tcp
    ufw allow 9050/tcp
    ufw allow 8060/tcp
    ufw allow 8060/tcp

##### 启动BE
在terminal运行如下命令  
`bash /alpha-suite/doris/be/bin/start_be.sh --daemon`

如遇报错：  
Please set vm.max_map_count to be 2000000 under root using 'sysctl -w vm.max_map_count=2000000'.  
在terminal运行如下命令  
`vim /etc/sysctl.conf`  
在打开的文件中，增加如下内容  
`vm.max_map_count=2000000`  
保存文件后，在terminal中运行以下命令使更改生效  
`sysctl -p`  
如果设置成功，会返回如下内容  
vm.max_map_count = 2000000

如遇报错：  
`Please disable swap memory before installation.`  
在terminal中运行如下命令  
`swapoff -a`  

##### 添加BE节点到集群
登陆mysql，在mysql中运行如下命令  
`ALTER SYSTEM ADD BACKEND "x.x.x.x:9050";`  
其中，x.x.x.x为本机ip，此处可以设置为127.0.0.1

##### 在mysql中查看BE状态。
在mysql中运行如下命令  
`SHOW BACKENDS\G;`  
如果Alive : true表示节点运行正常

### 重启电脑后启动FE、BE
重启后需在terminal中运行如下命令  

    source /etc/profile
    swapoff -a
    bash /alpha-suite/doris/fe/bin/start_fe.sh --daemon
    bash /alpha-suite/doris/be/bin/start_be.sh --daemon
启动FE后，需等待一段时间才能连上

为了简化重启后的流程，可以在/alpha-suite/startup/文件夹中放一个命名为startup.sh的bash脚本，将以上代码复制到bash脚本中，开机后在terminal中运行以下代码    
`bash /alpha-suite/startup/startup.sh`
