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
from text_seg import tokenize
from bs4 import BeautifulSoup
from urlparse import urlparse
from db import db
from os.path import join, getsize
from pymongo.errors import DuplicateKeyError

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

class BaseExtractor(ContentExtractor):
    '''
    extract content for long documents
    '''
    def __init__(self, file_path):
        ContentExtractor.__init__(self, file_path)
        return

    def __str__(self):
        tokens = [t.encode('utf-8') for t in self.document['tokens']]
        s = '/'.join(tokens)
        return s

    def parse_document(self):
        try:
            document = {}
            raw = self.source.readlines()

            document['source_url'] = raw[0].strip()
            document['timestamp'] = datetime.datetime.fromtimestamp(float(raw[1].strip()))
            document['domain'] = urlparse(document['source_url']).netloc

            html = ''.join(raw[2:]).strip()

            text = extract(html)
            document['tokens'] = tokenize(text)
        except Exception as e:
            print e, 'Some error has occured, continue'
            return False

        # print document['text']
        if self.verify_document(document):
            self.document = document
        else:
            return False
        return True

    def verify_document(self, document):
        '''
        verify whether the document is a corrected one
        '''
        if len(document['tokens']) == 0:
            print 'Invalid document...'
            return False
        else:
            print 'Valid document...'
            return True

    def insert(self):
        print 'inserting', self.collection.insert(self.document)
        return

class BlogExtractor(BaseExtractor):
    def __init__(self, file_path):
        BaseExtractor.__init__(self, file_path)
        self.collection = db.blog
        return

    # def parse_document(self):
    #     article = {}
    #     raw = self.source.readlines()
    #     surl, time_str, rurl = raw[0:3]
    #     time_str = time_str[0:time_str.find('.')]
    #     article['source_url'] = surl.strip()
    #     article['ref_url'] = rurl.strip()
    #     article['domain'] = urlparse(article['source_url']).netloc
    #     html = ''.join(raw[3:])
    #     article['text'] = extract(html)
    #     article['tokens'] = tokenize(article['text'])
    #     article['timestamp'] = datetime.datetime(*time.strptime(time_str.strip(), '%Y-%m-%d %H:%M:%S')[0:6])
    #     self.article = article
    #     return True

    # def insert(self):
    #     print 'inserting', self.collection.insert(self.article)
    #     return

class NewsExtractor(BaseExtractor):
    def __init__(self, file_path):
        BaseExtractor.__init__(self, file_path)
        self.collection = db.news
        return

class MagazineExtractor(BaseExtractor):
    def __init__(self, file_path):
        BaseExtractor.__init__(self, file_path)
        self.collection = db.magazines
        return

class BBSExtractor(BaseExtractor):
    def __init__(self, file_path):
        BaseExtractor.__init__(self, file_path)
        self.collection = db.bbs
        return

class WeiboExtractor(ContentExtractor):
    def __init__(self, file_path):
        '''
        file_path: full file path
        '''
        ContentExtractor.__init__(self, file_path)
        self.collection = db.weibo
        self.file_path = file_path
        self.source = open(self.file_path, 'r')

    def parse_document(self):
        weibo = {}


        # parse the key words
        try:
            raw = self.source.read()
            weibo  = json.loads(raw)

            weibo['src_file'] = self.file_path # used to check duplicate insert

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
        except ValueError:
            print 'value error'
            return False


        self.weibo = weibo
        # debug info
        print_dict(self.weibo)
        return True

    def insert(self):
        print 'inserting', self.collection.insert(self.weibo)
        return



class ExtractorFactory():
    '''
    return different extractor based on the filepath
    '''
    @staticmethod
    def get_extractor(file_path):
        path_conf = {
                # the key id the list of paths of the corresponding extractor
                Config.get('blog', 'path'): BlogExtractor,
                Config.get('news', 'path'): NewsExtractor,
                Config.get('bbs', 'path'): BBSExtractor,
                Config.get('weibo', 'path'): WeiboExtractor,
                Config.get('magazines', 'path'): MagazineExtractor
                # Config.get('general', 'path'): NormalExtractor
                }
        # use the longest prefix match to get the relevent extractor
        for key in path_conf:
            for dir_path in key.split(':'):
                if dir_path in file_path:
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
    count = 0
    start_time = time.time()
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            file_path = join(dirpath, filename)

            extension = os.path.splitext(file_path)[-1]
            print 'processing', file_path
            extractor = ExtractorFactory.get_extractor(file_path)

            if extractor is not None and extension in ['.txt']: # only parse the txt file
                print extractor.__class__.__name__
                try:
                    if extractor.parse_document():
                        extractor.insert()
                        count += 1
                    else:
                        print 'parser error for %s, continue' % (file_path,)
                except DuplicateKeyError:
                    print 'document existed, continue'
                    continue
            else:
                continue
    end_time = time.time()

    print 'Have processed %d files in %f seconds' % (count, end_time-start_time)
    return

def test(input_file = '/data/ywangby/workspace/pingan/data/content/1/11_0a00aef62518d03e196292b91a2f7df4.txt'):
    ne= NewsExtractor(input_file)
    ne.parse_document()
    print ne
    return

if __name__ == '__main__':
    test()
    # if len(sys.argv)>1:
    #     test(sys.argv[1])
    # else:
    #     test()
