# coding=utf-8
import urllib
import urllib2
import urlparse
import re
import time
import sys
import random
from bs4 import BeautifulSoup
from weibo import Client
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

search_domain = 's.weibo.com'
weibo_type = ('hot', 'time')

# setup browser driver
driver = webdriver.PhantomJS()
# driver.set_window_size(1440, 900)

# config the app data for authorization
APP_KEY = '185362843'
APP_SECRET = 'd6669fc08c76edbb0c300f7f238a8373'
CALLBACK_URI = 'http://keithyue.github.io'
APP_DATA = (APP_KEY, APP_SECRET, CALLBACK_URI)

USER_NAME = 'buaakeith@163.com'
PASSWD = '5805880'

class WeiboLogin():
    def __init__(self, username, passwd, driver):
        self.username = username
        self.passwd = passwd
        self.driver = driver

    def login(self):
        self.driver.get('http://www.weibo.com/login.php')
        try:
            WebDriverWait(self.driver, 10).until(
                    lambda x: x.find_element_by_css_selector('div.info_list')
                    )
            # print self.driver.page_source
            self.driver.maximize_window()
            self.driver.get_screenshot_as_file('./screenshot.png')
            user_input = self.driver.find_element_by_xpath('//div[@node-type="normal_form"]//input[@name="username"]')

            # print user_input.get_attribute('action-data')
            user_input.click()
            user_input.clear()
            user_input.send_keys(self.username)

            passwd_input = self.driver.find_element_by_xpath('//div[@node-type="normal_form"]//input[@name="password"]')
            passwd_input.clear()
            # print passwd_input
            passwd_input.send_keys(self.passwd)

            submit_button = self.driver.find_element_by_xpath('//div[@node-type="normal_form"]//a[@class="W_btn_g"]')
            submit_button.click()
        except TimeoutException:
            print 'load login page failed'
            return False
        try:
            WebDriverWait(self.driver, 5).until(
                    lambda x: x.find_element_by_class_name('WB_left_nav')
                    )
            print 'login success'
            return True
        except TimeoutException:
            print 'login failed', self.driver.current_url
            self.driver.get_screenshot_as_file('./login_failed.png')
            return False

    def authorize_app(self, app_data):
        '''
        authorize the app
        return the client for invoding weibo api
        '''
        if self.login():
            c = Client(*app_data)
            self.driver.get(c.authorize_url)
            try:
                WebDriverWait(driver, 10).until(
                        lambda x: x.find_element_by_css_selector('div.oauth_login_submit')
                        )
                # print driver.page_source
                submit_button = driver.find_element_by_css_selector('p.oauth_formbtn').find_element_by_tag_name('a')

                submit_button.click()
            except TimeoutException:
                # there is no submit button, so the user may have authorized the app
                print 'the user has authorized the app'

            # parse the code
            # print driver.current_url
            query_str = urlparse.urlparse(driver.current_url).query
            code = urlparse.parse_qs(query_str)['code']

            c.set_code(code)
            print 'authorize the app success! code,', code
            return c
        else:
            print 'login failed'
            return None

class WeiboCrawler():
    '''
    crawl weibo using keywords
    '''
    def __init__(self, search_key, driver):
        # login to sinaweibo
        self.driver = driver
        self.wl = WeiboLogin(USER_NAME, PASSWD, driver)
        if self.wl.login():
            print 'login successfully'
        else:
            print 'login faied'
            sys.exit(1)
        self.sk = search_key.strip()
        return

    def crawl(self, page_count=1):
        '''
        '''
        all_mids = []
        # get the mids from each result page
        pages = range(1, page_count+1)
        random.shuffle(pages)

        for i in pages:
            url_to_crawl = self.get_search_url(i)
            print 'crawling page', i
            self.driver.get(url_to_crawl)
            # wait the page loading the content
            try:
                element = WebDriverWait(self.driver, 10).until(
                        lambda x: x.find_elements_by_class_name('feed_list')
                        )
            except TimeoutException:
                print 'there is no weibo content in', url_to_crawl
                print 'you are considered as a robot'
                print driver.page_source
                print driver.current_url
                self.driver.get_screenshot_as_file('./error.png')
                break
            mids = self.parse_mids(self.driver.page_source) # mid is used to crawl the original weibo content, using batch mode
            all_mids.extend(mids)

            # sleep some time to prevent hitting too much
            time.sleep(5)

        print len(all_mids)
        print all_mids
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


    def parse_mids(self, content):
        '''
        given the content of a crawled weibo webpage,
        return the mids of all weibo results
        '''
        mids = []
        soup = BeautifulSoup(content, 'html5lib')
        for t in soup.find_all('dl', class_='feed_list'):
            if t.has_attr('mid'):
                # print t['mid']
                mids.append(t['mid'])
        print len(mids)
        return mids

    def get_banch_weibo(self, mids):
        '''
        given a list of mid
        return the list of original weibo
        '''
        c = wl.authorize_app(APP_DATA)
        return


def test():
    wc = WeiboCrawler('恒生银行', driver)
    wc.crawl(5)
    # wl = WeiboLogin(USER_NAME, PASSWD, driver)
    # c = wl.authorize_app(APP_DATA)
    # print c.get('users/show', uid=1282440983)

if __name__ == '__main__':
    test()
