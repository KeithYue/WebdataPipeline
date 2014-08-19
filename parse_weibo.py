# coding=utf-8
# parse data of weibo for external use
import logging
from content_extractor import update_socialmedial

def update_weibodata():
    update_socialmedial('weibo_data', 'weibo')
    return

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    update_weibodata()
