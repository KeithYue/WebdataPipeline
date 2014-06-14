#coding:utf-8
import os
import json
import ConfigParser
import re
import urllib
import extractor
import time
import datetime
import codecs
import sys
from pprint import pprint
from text_seg import *
from bs4 import BeautifulSoup
from urlparse import urlparse
from db import db
from os.path import join, getsize

# init the config file
Config = ConfigParser.ConfigParser()
Config.read('./config.ini')

def url_extract(url):
    content = urllib.urlopen(url).read()
    return extract(content)

def extract(content):
    if content is None:
        return None
    else:
        content = extractor.remove_js_css(content)
        soup = BeautifulSoup(content)
        return extractor.extract(soup.get_text())


class ContentExtractor():
    '''
    extract content from crawled files
    This a base class
    '''
    def __init__(self, file_path):
        self.source = open(file_path, 'r')
        return

    def __str__(self):
        return

    def parse_document(self):
        '''
        given a raw file
        return the parsed document represented in mongodb
        '''
        pass

    def insert(self):
        '''
        insert the document to mongodb
        '''
        pass

class BlogExtractor(ContentExtractor):
    def __init__(self, file_path):
        ContentExtractor.__init__(self, file_path)
        self.collection = db.blog
        self.parse_document()
        return

    def __str__(self):
        tokens = [t.encode('utf-8') for t in self.article['tokens']]
        s = '/'.join(tokens)
        return s

    def parse_document(self):
        article = {}
        raw = self.source.readlines()
        surl, time_str, rurl = raw[0:3]
        time_str = time_str[0:time_str.find('.')]
        article['source_url'] = surl.strip()
        article['ref_url'] = rurl.strip()
        article['domain'] = urlparse(article['source_url']).netloc
        html = ''.join(raw[3:])
        article['text'] = extract(html)
        article['tokens'] = tokenize(article['text'])
        article['timestamp'] = datetime.datetime(*time.strptime(time_str.strip(), '%Y-%m-%d %H:%M:%S')[0:6])
        self.article = article
        return

    def insert(self):
        print 'inserting', self.collection.insert(self.article)
        return

class NewsExtractor(ContentExtractor):
    pass

class WeiboExtractor(ContentExtractor):
    def __init__(self, file_path):
        '''
        file_path: full file path
        '''
        ContentExtractor.__init__(self, file_path)
        self.collection = db.weibo
        self.file_path = file_path
        self.source = open(self.file_path, 'r')
        self.parse_document()

    def parse_document(self):
        self.weibo = {}

        raw = self.source.read()
        weibo  = json.loads(raw)

        weibo['src_file'] = self.file_path # used to check duplicate insert

        # parse the key words
        full_dir_path = os.path.split(self.file_path)[0]
        dir_name = os.path.basename(full_dir_path)
        weibo['keywords'] = dir_name.split(' ')
        weibo['tokens'] = tokenize(weibo['content'])

        # construct timestamp
        actual_date_str = u'2014年'+weibo['date'].strip()
        time_tuple = time.strptime(actual_date_str, u'%Y年%m月%d日 %H:%M')
        weibo['timestamp'] = datetime.datetime(*time_tuple[0:6])

        # heat of weibo
        weibo['heat'] = weibo['heat'].replace('\n', '')
        pattern = re.compile(ur'.*转发\((?P<retweet_num>\d+)\).*')
        match = pattern.match(weibo['heat'])
        if match:
            weibo['retweet'] = int(match.groupdict()['retweet_num'])
        else:
            weibo['retweet'] = 0


        self.weibo = weibo
        # debug info
        print_dict(self.weibo)
        return

    def insert(self):
        print 'inserting', self.collection.insert(self.weibo)
        return

class BBSExtractor(ContentExtractor):
    pass


class ExtractorFactory():
    '''
    return different extractor based on the filepath
    '''
    @staticmethod
    def get_extractor(file_path):
        path_conf = {
                Config.get('blog', 'path'): BlogExtractor,
                Config.get('news', 'path'): NewsExtractor,
                Config.get('bbs', 'path'): BBSExtractor,
                Config.get('weibo', 'path'): WeiboExtractor
                }
        for key in path_conf:
            if key in file_path:
                return path_conf[key](file_path)
        return None

def print_dict(d):
    '''
    print the fields of dictionary

    d: dict
    '''
    for key in d:
        print key+':',
        if type(d[key]) == list:
            for i in d[key]:
                print i,
        else:
            print d[key],

        print
    return

def main():
    '''
    the pipeline main program
    walk through the filesystem, find data file parse them and insert them to database
    '''
    root = Config.get('root', 'path')
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            file_path = join(dirpath, filename)
            extractor = ExtractorFactory.get_extractor(file_path)
            if extractor is not None:
                print extractor.__class__.__name__
                extractor.insert()
            else:
                continue

def test(input_file = '/data/ywangby/workspace/pingan/data/weibo/能源局 亿元现金/能源局 亿元现金/1401369020.58.txt'):
    ce= WeiboExtractor(input_file)
    # print ce.article['source_url']
    return

if __name__ == '__main__':
    test()
    # if len(sys.argv)>1:
    #     test(sys.argv[1])
    # else:
    #     test()
