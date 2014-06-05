# coding=utf-8
from login import login
from bs4 import BeautifulSoup
import urllib
import urllib2

search_domain = 's.weibo.com'
weibo_type = ('hot', 'time')

class WeiboCrawler():
    '''
    crawl weibo using keywords
    '''
    def __init__(self, search_key='银行'):
        # login to sinaweibo
        print 'loging into sina weibo'
        login()
        self.sk = search_key.strip()
        return

    def crawl(self, page_count=1):
        '''
        '''
        for i in range(1, page_count+1):
            url_to_crawl = self.get_search_url(i)
            print 'crawling', url_to_crawl
            try:
                content = urllib2.urlopen(url_to_crawl).read()
                self.parse_page(content)
            except Exception as e:
                print 'Some error has occured'
                print e
        return

    def get_search_url(self, page=1, w_type='hot'):
        '''
        compose a search url based on page_num and weibo type
        '''
        print 'generating the url'
        url=''
        url += 'http://'
        url += search_domain
        url += '/wb'
        url += urllib.quote('/'+self.sk)
        url += '&'
        url += urllib.urlencode([
            ('page', page),
            ('xsort', w_type)
            ])

        return url

    def parse_page(self, content):
        '''
        given the content of a crawled weibo webpage,
        return the structual weibo content
        '''
        soup = BeautifulSoup(content, 'html5lib')
        print soup.original_encoding
        # print soup.prettify()
        for t in soup.find_all(class_='feed_lists'):
            print t.name

        return

def test():
    wc = WeiboCrawler('银行')
    wc.crawl()

if __name__ == '__main__':
    test()
