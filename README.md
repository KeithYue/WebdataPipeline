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
1. `git clone https://github.com/KeithYue/WebdataPipeline.git` into workspace and `cd WebdataPipeline`
2. edit the config.ini file, in which you need to config the ip:port of mongodb instance and where the raw data has been stored, different kinds of data has been mapped to different directories.
3. run `python main.py`. The program would use the `cpu_count-2` numbers of cores in the current machine.
4. example of output:
![example](https://www.dropbox.com/s/n3dxyq1y9f7t9ky/wbpipeline_sample.png)

