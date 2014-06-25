WebdataPipeline
===============

construct pipeline for different forms of web data such as weibo, bbs, news, blog. Including spider, content extraction, tokenize

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
1. edit the config.ini file, in which you need to config the ip:port of mongodb instance and where the raw data has been stored, different kinds of data has been mapped to different directories.
2. run `python main.py`. The program would use the `cpu_count-2` numbers of cores in the current machine.

