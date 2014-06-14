# coding=utf-8
from pymongo import MongoClient

client = MongoClient()
db = client['pingan']
# articles = db.articles
# news = db.news

# create the db index
db.blog.ensure_index('source_url', cache_for=300, unique=True, dropDups=True)
db.weibo.ensure_index('src_file', cache_for=300, unique=True, dropDups=True)


if __name__ == '__main__':
    pass

