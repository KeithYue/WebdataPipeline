# coding=utf-8
import logging
from content_extractor import update_socialmedial

def parse_bbs():
    update_socialmedial('bbs_data', 'bbs')
    return

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parse_bbs()
