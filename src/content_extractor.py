#coding:utf-8
import os
import ConfigParser
import re
import urllib
import extractor
import time
import datetime
import codecs
import sys
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
        article['html'] = ''.join(raw[3:])
        article['text'] = extract(article['html'])
        article['tokens'] = tokenize(article['text'])
        article['timestamp'] = datetime.datetime(*time.strptime(time_str.strip(), '%Y-%m-%d %H:%M:%S')[0:6])
        self.article = article
        return

    def insert(self):
        self.collection.insert(self.article)
        return

class NewsExtractor(ContentExtractor):
    pass

class WeiboExtractor(ContentExtractor):
    pass

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
            else:
                continue

def test(input_file = './test1.txt'):
    ce= BlogExtractor(input_file)
    print ce
    print ce.article['source_url']
    return

if __name__ == '__main__':
    main()
    # if len(sys.argv)>1:
    #     test(sys.argv[1])
    # else:
    #     test()
