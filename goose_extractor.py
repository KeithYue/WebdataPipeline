# coding: utf-8
import re
import time
import urllib2
from goose import Goose
from goose.text import StopWordsChinese

def extract_content(html):
    g = Goose({'stopwords_class': StopWordsChinese})
    article = g.extract(raw_html=html)
    return article.cleaned_text

def test():
    url = 'http://news.163.com/09/1109/02/5NL6V0VB000120GU.html'
    raw_html = urllib2.urlopen(url).read()
    print extract_content(raw_html)

if __name__=='__main__':
    test()
