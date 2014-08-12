#coding:utf-8
import os
import json
import ConfigParser
import re
import urllib
import urllib2
import extractor
import time
import datetime
import codecs
import sys
import logging
from pprint import pprint
from text_seg import tokenize, print_tokens
from bs4 import BeautifulSoup
from urlparse import urlparse
from db import db
from os.path import join, getsize
from pymongo.errors import DuplicateKeyError
from goose_extractor import extract_content

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
        # content = extractor.remove_js_css(content)
        # soup = BeautifulSoup(content)
        # return extractor.extract(soup.get_text())
        c = extract_content(content)
        # print c
        return c

def udpate_weibodata():
    '''
    the weibo data is a little bit different, weibo data is already in the database
    '''
    for weibo in db.weibo_data.find({'tokens': {
        '$exists': 0
        }}):
        try:
            tokens = tokenize(weibo['value']['content'])
            logging.info(weibo['_id'])
            # logging.info('updating the weibo %s' % weibo['value']['mid'])
            weibo['text'] = weibo['value']['content']
            weibo['tokens'] = tokens
            weibo['timestamp'] = datetime.datetime.fromtimestamp(weibo['value']['created_at'])
            # update the document
            db.weibo_data.update({'_id': weibo['_id']}, weibo)
            a = raw_input()
        except Exception as e:
            logging.error('updating failure, continue...')
            continue
    return True

def parse_time(s):
    '''
    try to parse a string into datetime --> datetime.datetime

    s: the time string
    '''
    pattern2 = re.compile(r'''
            ^ # the start of the string
            (\d+) # the year
            \D+
            (\d+) # the month
            \D+
            (\d+) # the day
            \D+
            (\d+) # the hour
            \D+
            (\d+) # the minute
            \D+
            (\d+) # the seconds
            $ # the end of the string
            ''', re.VERBOSE)

    if re.search(r'^\d+\.\d+$', s):
        return datetime.datetime.fromtimestamp(float(s))

    elif pattern2.search(s):
        m = pattern2.search(s)
        time_list = [int(i) for i in m.groups()]
        return datetime.datetime(*time_list)
    else:
        print 'Parse time error'
        return None


class ContentExtractor():
    '''
    extract content from crawled files
    This a base class
    '''
    def __init__(self, file_path):
        self.source = open(file_path, 'r')
        # print file_path
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
        self.file_path = file_path
        return

    def __str__(self):
        tokens = [t.encode('utf-8') for t in self.document['tokens']]
        s = '/'.join(tokens)
        return s

    def is_parsed(self):
        '''
        whether this file has been parsed before --> True or False
        '''
        if self.collection.find_one({'src_file': self.file_path}):
            return True
        else:
            return False

    def parse_document(self):
        try:
            document = {}
            raw = self.source.readlines()

            document['src_file'] = self.file_path

            document['source_url'] = raw[0].strip()
            document['timestamp'] = parse_time(raw[1].strip())
            document['domain'] = urlparse(document['source_url']).netloc

            html = ''.join(raw[2:]).strip()

            text = extract(html)
            document['text'] = text
            document['tokens'] = tokenize(text)
            document['new_attr'] = 1
            # print document['tokens']
        except Exception as e:
            # print e, 'Some error has occured, continue'
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
            print 'Invalid document..., there is no tokens and content in this page'
            return False
        else:
            print 'Valid document...'
            return True

    def insert(self):
        print 'inserting', self.collection.insert(self.document)
        return

    def update(self, ignore_parsed=True):
        '''
        store the document in the database

        ignore_parsed: whether update all documents
            true: ignore if the document exists in the database
            false: update all the document

        return 1 when a document has been added
        return 0 when no document has been added
        '''
        if ignore_parsed:
            if self.is_parsed():
                logging.info('Document exists, next...')
                return 0
            else:
                self.collection.update({'src_file': self.file_path}, self.document, True)
                return 1
        else:
            self.collection.update({'src_file': self.file_path}, self.document, True)
            return 1


class BlogExtractor(BaseExtractor):
    def __init__(self, file_path):
        BaseExtractor.__init__(self, file_path)
        self.collection = db.blog
        return

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
            logging.error('value error')
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

def test():
    '''
    test the content extractor
    '''
    # document = urllib2.urlopen('http://news.163.com/09/1109/02/5NL6V0VB000120GU.html').read()
    # print extract(document)
    udpate_weibodata()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test()
