WebdataPipeline
===============

该工具主要用来解析原始的爬虫数据文件，功能包括：
* 从原始html中提取正文
* 对正文进行分词
* 将解析出来的格式存入mongodb数据库
* 目前数据源有博客，新闻和微博

### 使用方法
1. `python main.py` 用来解析txt文本文件(不包括微博数据)
2. `python parse_weibo.py` 用来解析在weibo_data collection里面的微博数据, 并将数据放入weibo collection中。

### Dependencies

#### Environment
* Anaconda
* mongodb

#### Python modules
* Python 2.7
* BeautifulSoup
* jieba
* pymongo

### Usage
1. `git clone https://github.com/KeithYue/WebdataPipeline.git` into workspace and `cd WebdataPipeline`
2. edit the config.ini file, in which you need to config the ip:port of mongodb instance and where the raw data has been stored, different kinds of data has been mapped to different directories.
3. run `python main.py`. The program would use the `cpu_count-2` numbers of cores in the current machine.
4. example of output:

![example](http://keithyue.github.io/images/wbpipeline_sample.png)

