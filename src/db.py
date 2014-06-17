# coding=utf-8
from pymongo import MongoClient

client = MongoClient()
db = client['pingan']
# articles = db.articles
# news = db.news

# create the db index
db.blog.ensure_index('source_url', cache_for=300, unique=True, dropDups=True)
db.news.ensure_index('source_url', cache_for=300, unique=True, dropDups=True)
db.magazines.ensure_index('source_url', cache_for=300, unique=True, dropDups=True)
db.bbs.ensure_index('source_url', cache_for=300, unique=True, dropDups=True)
db.weibo.ensure_index('src_file', cache_for=300, unique=True, dropDups=True)


def clear_db():
    collections = ['blog', 'news', 'magazines', 'bbs']
    for c in collections:
        db[c].drop()
    return

if __name__ == '__main__':
    clear_db()

