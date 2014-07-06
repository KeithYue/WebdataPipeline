# coding=utf-8
import ConfigParser
from pymongo import MongoClient

# read the config file
Config = ConfigParser.ConfigParser()
Config.read('./config.ini')
bind_ip = Config.get('mongo', 'bind_id')
port = Config.getint('mongo', 'port')


client = MongoClient(bind_ip, port)
db = client['pingan']
# articles = db.articles
# news = db.news

# create the db index
db.blog.ensure_index('src_file', cache_for=300, unique=True, dropDups=True)
db.news.ensure_index('src_file', cache_for=300, unique=True, dropDups=True)
db.magazines.ensure_index('src_file', cache_for=300, unique=True, dropDups=True)
db.bbs.ensure_index('src_file', cache_for=300, unique=True, dropDups=True)
db.weibo.ensure_index('src_file', cache_for=300, unique=True, dropDups=True)


def clear_db():
    collections = ['blog', 'news', 'magazines', 'bbs']
    for c in collections:
        print 'dropping', c
        db[c].drop()
    return

if __name__ == '__main__':
    clear_db()
