# -*- coding: utf-8 -*-
import hashlib
import os
import traceback
import json
from urllib import parse
import math

import requests
from bs4 import BeautifulSoup
from requests.cookies import RequestsCookieJar
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from apscheduler.schedulers.blocking import BlockingScheduler
from script.houniao.common import HNCommonParam
from script.houniao.houniao_bo import *
from script.houniao.logging_utils import Logger

class DriverUtil():
    base_dir = os.path.dirname(__file__)

    def get_driver(self):
        co = webdriver.ChromeOptions()
        co.add_extension(os.path.join(self.base_dir, 'ivimm_chrome_proxyauth_plugin.zip'))
        driver = webdriver.Chrome(os.path.join(self.base_dir, 'chromedriver.exe'), chrome_options=co)
        return driver


class HouNiaoScript():

    headers = {
        'key': '00cad0fb5df34fbad99981b6c406d39e',
        'Content-Type': 'application/x-www-form-urlencoded',
        'accept': 'application/json',
        'user-agent': 'MIX 2(Android/8.0.0) (com.houniao.hk/1.0.4) Weex/0.26.0 1080x2030',
    }


    base_dir = os.path.dirname(__file__)
    base_url = 'http://www.houniao.hk'
    login_url = base_url + '/home/User/login'
    cookie_ts_key = 'Hm_lpvt_79b5f26a1df74b42929511d91ac22f32'
    download_path = os.path.join(HNCommonParam.script_download_path, time.strftime("%Y%m%d%H%M%S", time.localtime()))
    cookies_jar = None

    def login(self, driver):
        if driver is None or ('home/User/login' not in driver.current_url and 'data:,' != driver.current_url):
            return False
        try:
            driver.get(self.login_url)
            WebDriverWait(driver, 60 * 5, 3).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.userName')))
            time.sleep(1.6)
            driver.find_element_by_class_name('userName').send_keys(HNCommonParam.hn_username)
            time.sleep(1.6)
            driver.find_element_by_class_name('password').send_keys(HNCommonParam.hn_password)
            time.sleep(1.6)
            cnt = 0
            while driver.find_element_by_class_name('submit-form').is_displayed():
                if cnt > 10:
                    self.login(driver)
                cnt = cnt+1
                driver.find_element_by_class_name('submit-form').click()
                time.sleep(1.8)
            if not driver.find_element_by_class_name('submit-form').is_displayed():
                WebDriverWait(driver, 60 * 1, 3).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.verify-code')))
                verify_exp = driver.find_element_by_class_name('verify-code').text.replace('=', '').replace('?', '').replace('×', '*')
                verify_exp_result = eval(verify_exp)
                Logger.log('验证码为：%s, 计算结果为：%s' % (verify_exp, verify_exp_result))
                driver.find_element_by_class_name('varify-input-code').send_keys(verify_exp_result)
                time.sleep(0.6)
                driver.find_element_by_class_name('cerify-code-button').click()
                WebDriverWait(driver, 60*2, 3).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.personal')))
                Logger.log('登录成功！')
        except Exception as err:
            traceback.print_exc()
            Logger.log('登录失败,重新登录： %r ' %(err))
            self.login(driver)
        return True

    def close_ad_window(self, driver):
        if driver.find_element_by_class_name('product-huodong-close').is_displayed():
            driver.find_element_by_class_name('product-huodong-close').click()


    def init_cookies(self):
        self.cookies_jar = RequestsCookieJar()
        with  DriverUtil().get_driver() as driver:
            self.login(driver)
            cookies = driver.get_cookies()
            for cookie in cookies:
                self.cookies_jar.set(cookie['name'], cookie['value'])
            self.cookies_jar.set('productHuodongClose', 'yes')
            self.cookies_jar.set(self.cookie_ts_key, int(time.time()))

    # @retry(retry_on_exception=BaseException, wrap_exception=False, stop_max_attempt_number=5, wait_random_min=2000,
    #        wait_random_max=5000)
    def request_get_proxy(self, url, params=None):
        Logger.log('request get:' + url)
        Logger.log(params)
        self.cookies_jar.set('productHuodongClose', 'yes')
        self.cookies_jar.set(self.cookie_ts_key, str(int(time.time())))
        while True:
            response = requests.get(url=url, params=params, cookies= self.cookies_jar, timeout= 60*5, verify=False)
            self.download_file(response)
            if response.status_code == 200:
                break
            else:
                Logger.log('%s请求失败：%d,重试'% (url, response.status_code))
                time.sleep(5)
        return response

    def request_proxy(self, url, params=None, headers=None, cookies=None):
        Logger.log('request get:' + url)
        Logger.log(params)
        while True:
            response = requests.get(url=url, params=params, headers=headers, cookies= cookies, timeout= 60*5, verify=False)
            self.download_file(response)
            if response.status_code == 200:
                break
            else:
                Logger.log('%s请求失败：%d,重试'% (url, response.status_code))
                time.sleep(5)
        return response


    def download_countdown_good_lists(self, url=None, activity_pid=None, activity_code=None):
        res = self.request_get_proxy(url)
        soup = BeautifulSoup(res.text)
        activity_goods = soup.select('div.activity-goodslist div.detail-wrap')
        for group_good in activity_goods:
            good = GoodBo()
            good.activity_pid = activity_pid
            good.activity_code = activity_code
            good.url = self.base_url + group_good.select_one('.goods-info .goods-link')['href']
            good.name = group_good.select_one('.goods-info .goods-link .goods-title').text
            good.code = self.get_code_from_href(group_good.select_one('.goods-info .goods-link')['href'])
            if self.download_pic_url(group_good.select_one('.goods-info .goods-link img.goods-image')['src'], file_name=good.code):
                good.pic_url = self.base_url + group_good.select_one('.goods-info .goods-link img.goods-image')['src']
            GoodDao.insert(good)


    def download_card_good_lists(self, url=None, activity_pid=None, activity_code=None):
        res = self.request_get_proxy(url)
        soup = BeautifulSoup(res.text)
        group_goods = soup.select('.result.module-floor')
        for group_good in group_goods:
            group_title = group_good.select_one('.floor-name')
            goods = group_good.select('.floor-detail div.goods-col')
            for good_item in goods:
                good = GoodBo()
                good.activity_pid = activity_pid
                good.activity_code = activity_code
                good.sub_title = group_title.text.strip()
                good.name = good_item.select_one('.goods-info .goods-name a')['title']
                good.url = self.base_url + good_item.select_one('.goods-info .goods-name a')['href']
                good.code = self.get_code_from_href(good_item.select_one('.goods-info .goods-name a')['href'])
                if self.download_pic_url( soup.select_one('.goods-img img')['data-original'], file_name=good.code):
                    good.pic_url = self.base_url + soup.select_one('.goods-img img')['data-original']
                GoodDao.insert(good)

    def download_good_item_lists(self, list_url=None, activity_pid=None, activity_code=None):
        res = self.request_get_proxy(list_url)
        soup = BeautifulSoup(res.text)
        brand_title = soup.select_one('.brand-main .brand-title')
        brand_product_items = soup.select('.brand-product .product-main .product-items')
        for brand_product_item in brand_product_items:
            good = GoodBo()
            good.activity_pid = activity_pid
            good.activity_code = activity_code
            good.sub_sub_title = brand_title.text.strip()
            good.sub_title = self.base_url +  brand_title.select_one('img')['src']
            # good.price = brand_product_item.select_one('.product-price').text.strip()
            product_info = brand_product_item.select_one('.product-name a')
            good.name = product_info.text.strip()
            good.url = self.base_url + product_info['href']
            good.code = self.get_code_from_href(product_info['href'])
            if self.download_pic_url(brand_product_item.select_one('.product-img img')['src'], file_name=good.code):
                good.pic_url = self.base_url + brand_product_item.select_one('.product-img img')['src']
            GoodDao.insert(good)


    def download_search_app_good_list(self, keyword=None, activity_pid=None, activity_code=None):
        url = 'https://www.houniao.hk/wxapi/goods/selectGoods'
        page = 1
        while True:
            params = {'page': page, 'limit': 8, 'keywords': keyword, 'sortType': '人气', 'tradeTypeId': 0, 'categoryIds': 0, 'categoryRank': 0}
            res = self.request_proxy(url=url, params=params,  headers=self.headers).json()
            data_count = res['data']['dataCount']
            for data in res['data']['list']:
                good_bo = GoodBo()
                good_bo.activity_code = activity_code
                good_bo.activity_pid = activity_pid
                good_bo.name = data['goodsName']
                good_bo.code = data['goodsSku']
                pic_url = '/'+ data['goodsImg']
                if self.download_pic_url(pic_url, file_name=good_bo.code):
                    good_bo.pic_url = self.base_url + pic_url
                GoodDao.insert(good_bo)
            page = page + 1
            if math.ceil(data_count / 8) < page:
                break

    def download_search_good_list(self, search_url=None, activity_pid=None, activity_code=None, limit_page_num=None):
        split_search_url = search_url[:search_url.index('?')]
        params = dict(parse.parse_qsl(search_url[search_url.index('?')+1:]))
        cur_num = 1
        first_run = True
        soup =None
        while first_run or (soup.select_one('#totalPage') is not None and cur_num < int(soup.select_one('#totalPage')['value'])):
            if first_run:
                first_run = False
            elif limit_page_num is not None and limit_page_num <= cur_num:
                return
            else:
                cur_num = cur_num+1
                params['index']=cur_num
            res = self.request_get_proxy(split_search_url, params= params)
            soup = BeautifulSoup(res.text)
            goods = soup.select('.result ul li.goods')
            for good in goods:
                good_bo = GoodBo()
                good_bo.activity_code = activity_code
                good_bo.activity_pid = activity_pid
                good_bo.name = good.select_one('.goods-name').text
                good_bo.url = self.base_url + good.select_one('.goods-name a')['href']
                good_img = good.select_one('.goods-img img')
                pic_url = good_img.get('data-original', good_img.get('src'))

                good_bo.code = self.get_code_from_href(self.base_url + good.select_one('.goods-name a')['href'])
                if self.download_pic_url(pic_url, file_name=good_bo.code):
                    good_bo.pic_url = self.base_url + pic_url
                GoodDao.insert(good_bo)

    def download_good_detail(self, detail_url, activity_pid=None, activity_code=None):
        res = self.request_get_proxy(detail_url)
        soup = BeautifulSoup(res.text)
        sku_name = soup.select_one('.goods-detail-center .sku-name')
        label_name = sku_name.select_one('.label').text
        sku_name = sku_name.text.replace(label_name, '')
        good_bo = GoodBo()
        good_bo.activity_code = activity_code
        good_bo.activity_pid = activity_pid
        good_bo.name = sku_name
        good_bo.url = detail_url
        good_bo.code = self.get_code_from_href(detail_url)
        if self.download_pic_url(soup.select_one('.img-con img')['src'], file_name=good_bo.code):
            good_bo.pic_url = self.base_url + soup.select_one('.img-con img')['src']
        GoodDao.insert(good_bo)

    def download_goods(self):
        # App Banner页
        app_banner = 'https://www.houniao.hk/wxapi/index/carousel'
        app_banner_res = self.request_proxy(url=app_banner, headers=self.headers).json()
        if app_banner_res['code'] == 200:
            for data in app_banner_res['data']:
                ad_name = data['adName']
                ad_url = data['adURL']
                ad_img_file =  "/" +  data['adFile']
                activity = ActivtiyBO()
                activity.type = 'search'
                activity.activity_code = 'TOP'
                activity.name = ad_name
                activity.pic_url = self.base_url +ad_img_file
                activity_pid = ActivtiyDao.insert(activity)
                self.download_pic_url(img_url=ad_img_file, file_name='TOP#顶幅banner#' + str(activity_pid))
                # 下载详情数据
                self.download_search_app_good_list(keyword=ad_name, activity_pid=activity_pid, activity_code='TOP')


        # App 国际馆
        coutry_url = 'https://www.houniao.hk/wxapi/index/country'
        coutry_res = self.request_proxy(url=coutry_url, headers= self.headers).json()
        if coutry_res['code'] == 200:
            for data in coutry_res['data']:
                ad_name = data['adName']
                ad_url = data['adURL']
                ad_img_file = '/' + data['adFile']
                activity = ActivtiyBO()
                activity.activity_code = 'COUNTRY'
                activity.name = ad_name
                activity_pid = ActivtiyDao.insert(activity)
                self.download_pic_url(img_url=ad_img_file, file_name='COUNTRY#' + ad_name + "#" + str(activity_pid))
                for page in [1,2]:
                    country_good_url = 'https://www.houniao.hk/wxapi/goods/selectGoods'
                    country_good_params = {'originId': ad_url, 'page':page,'limit':8}
                    country_good_res = self.request_proxy(url=country_good_url, params=country_good_params,  headers= self.headers).json()
                    if country_good_res['code'] ==200:
                        for good_list_item in country_good_res['data']['list']:
                            good_bo = GoodBo()
                            good_bo.activity_code = 'COUNTRY'
                            good_bo.activity_pid = activity_pid
                            good_bo.code = good_list_item['goodsSku']
                            good_id = good_list_item['goodsId']
                            good_bo.name = good_list_item['goodsName']
                            good_img = '/' + good_list_item['goodsImg']
                            if self.download_pic_url(good_img, file_name=good_bo.code):
                                good_bo.pic_url = self.base_url + good_img
                            GoodDao.insert(good_bo)
        else:
            Logger.log('国际馆下载失败')
        self.init_cookies()
        res = self.request_get_proxy('http://www.houniao.hk/')
        soup = BeautifulSoup(res.text)
        soup.select_one('.nav-tabs .nav-item')

        self.download_trade_type_goods(soup, '保税直供', 'TRADETYPE', 1)
        self.download_trade_type_goods(soup, '完税进口', 'TRADETYPE', 1)
        self.download_trade_type_goods(soup, '国内贸易', 'TRADETYPE', 1)
        self.download_trade_type_goods(soup, '香港直邮', 'TRADETYPE', 1)
        self.download_trade_type_goods(soup, '海外直邮', 'TRADETYPE', 1)

        # 网红爆品+新品上市
        Logger.log('开始下载 网红爆品')
        hot_good = soup.select_one('.nav-tabs .nav-item:contains("网红爆品")')
        activity = ActivtiyBO()
        activity.activity_code = 'HOT'
        activity.name = '网红爆品'
        activity.url = self.base_url+ hot_good.select_one('a')['href']
        activity_pid = ActivtiyDao.insert(activity)
        self.download_good_item_lists(activity.url, activity_pid, 'HOT')

        self.download_trade_type_goods(soup, '新品上市', 'NEW')
        # Logger.log('开始下载 新品上市')
        # new_good = soup.select_one('.nav-tabs .nav-item:contains("新品上市")')
        # activity = ActivtiyBO()
        # activity.activity_code = 'NEW'
        # activity.name = '新品上市'
        # activity.url = self.base_url+ new_good.select_one('a')['href']
        # activity_pid = ActivtiyDao.insert(activity)
        # self.download_search_good_list(activity.url, activity_pid, 'NEW')

        # pc顶幅, 废弃，使用APP跑马灯
        # Logger.log('开始下载 顶幅跑马灯')
        # banner_items = soup.select('.banner-slide .items li')
        # for banner_item in banner_items:
        #     activity = ActivtiyBO()
        #     href = banner_item.select_one('a')['href']
        #     activity.activity_code = 'TOP'
        #     activity.name = '顶幅banner'
        #     activity.url = href
        #     if banner_item['_src'] is not None:
        #         activity.pic_url = self.base_url + banner_item['_src'].replace('url(', '').replace(')', '')
        #     # type: # detail: 商品详情, search: 搜索结果页，other： 其他
        #     if 'product/detail' in href:
        #         # 商品详情链接：
        #         # if banner_item['style'] is not None:
        #         #     pass
        #         activity.type = 'detail'
        #         activity_pid = ActivtiyDao.insert(activity)
        #         self.download_good_detail(href, activity_pid=activity_pid, activity_code='TOP')
        #     elif 'product/search' in href:
        #         activity.type = 'search'
        #         activity_pid = ActivtiyDao.insert(activity)
        #         self.download_search_good_list(href, activity_pid=activity_pid, activity_code='TOP')
        #     else:
        #         Logger.log('unknow type:' + href)
        #         activity.type = 'other'
        #         activity_pid = ActivtiyDao.insert(activity)
        #     if not self.download_pic_url(banner_item['_src'].replace('url(', '').replace(')', ''), file_name='TOP#顶幅banner#' + str(activity_pid)):
        #         activity.pid = activity_pid
        #         activity.pic_url = None
        #         ActivtiyDao.update_room_detail(activity)

        # 卡片活动
        Logger.log('开始下载 卡片活动')
        floor_items = soup.select('.floor-items  .floor-item')
        for floor_item in floor_items:
            href = floor_item.select_one('a')['href']
            name = floor_item.select_one('p.name').text.strip()
            desc = floor_item.select_one('p.desc').text.strip()
            img_url = floor_item.select_one('img')['src']
            activity = ActivtiyBO()
            activity.activity_code = 'CARD'
            activity.desc = desc
            activity.name = '卡片活动-' + name
            activity.url = self.base_url+href
            activity.pic_url = self.base_url +img_url
            activity_pid = ActivtiyDao.insert(activity)
            if not self.download_pic_url(img_url, file_name='CARD#' + activity.name+'#' + str(activity_pid)):
                activity.pid = activity_pid
                activity.pic_url = None
                ActivtiyDao.update_room_detail(activity)
            self.download_card_good_lists(activity.url, activity_pid, 'CARD')
        # 抢购
        Logger.log('开始下载 抢购')
        activity = ActivtiyBO()
        activity.activity_code = 'COUNTDOWN'
        activity.name = '抢购'
        activity.url = self.base_url+ soup.select_one('#flashsale .navbox.active')['href']
        activity_pid = ActivtiyDao.insert(activity)
        self.download_countdown_good_lists(activity.url, activity_pid=activity_pid, activity_code='COUNTDOWN')

        # 爬取商品分类第一页
        Logger.log('开始爬取 商品分类')
        list_items = soup.select('.site-category .catlist li.list-item')
        for list_item in list_items:
            # :奶粉辅食
            item_name_1 = list_item.select_one('a.item span.catname').text.strip()
            for list_item2 in list_item.select('div.sub-list dl.slblock'):
                # 婴儿奶粉
                item_name_2 = list_item2.select_one('dt.li-title').text.strip()
                for list_item3 in list_item2.select('dd.li-item'):
                    item_name_3 = list_item3.text.strip()
                    activity = ActivtiyBO()
                    activity.activity_code = 'CATEGORY'
                    activity.name = item_name_1 + '-' + item_name_2 + '-' + item_name_3
                    activity.url = 'http:'+ list_item3.select_one('a')['href']
                    activity_pid = ActivtiyDao.insert(activity)
                    self.download_search_good_list(activity.url, activity_pid, 'CATEGORY', limit_page_num=1)


    def download_trade_type_goods(self, soup, target_title, activiy_code, limit_page_num=None):
        Logger.log('开始下载 '+ target_title)
        trade_good = soup.select_one('.nav-tabs .nav-item:contains("%s")' % (target_title))
        activity = ActivtiyBO()
        activity.activity_code = activiy_code
        activity.name = target_title
        activity.url = self.base_url+ trade_good.select_one('a')['href']
        activity_pid = ActivtiyDao.insert(activity)
        self.download_search_good_list(activity.url, activity_pid, activiy_code, limit_page_num=limit_page_num)

    def download_file(self, response):
        pass
        # if not os.path.exists(self.download_path):
        #     os.makedirs(self.download_path)
        # _hash = hashlib.md5()
        # _hash.update(response.url.encode('utf-8'))
        # md5_code = _hash.hexdigest()
        # with open(os.path.join(self.download_path, md5_code), 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        # return response

    def download_pic_url(self, img_url=None, file_name=None):
        try:
            full_img_url = self.base_url + img_url
            split_file_name = img_url.split('/')[-1]
            if file_name is None:
                file_name = split_file_name
            else:
                file_name = file_name + os.path.splitext(split_file_name)[-1]
            if img_url.startswith('/'):
                img_url = img_url[1:]
            download_path = os.path.join(HNCommonParam.script_download_path, img_url.replace(split_file_name, ''))
            if not os.path.exists(download_path):
                os.makedirs(download_path)
            r = requests.get(full_img_url, stream=True, timeout=60*5)
            abs_file_path = os.path.join(download_path, file_name)
            # print('保存图片：' + abs_file_path)
            with open(abs_file_path, 'wb') as f:
                f.write(r.content)
            return True
        except Exception as err:
            Logger.log('img %s download err' % img_url)
            return False

    def get_code_from_href(self, href):
        if href is None:
            return None
        return href[href.index('itemSku=')+len('itemSku='):]

scheduler = BlockingScheduler()
@scheduler.scheduled_job("cron", day_of_week='*', hour=HNCommonParam.job_hour, minute=HNCommonParam.job_min, second='00')
def rebate():
    script = HouNiaoScript()
    try:
        Logger.log('spider task start')
        script.download_goods()
        Logger.log('Spider task end')
    except BaseException as err:
        Logger.log('Spider task failed: ' + time.strftime('%Y-%m-%d', time.localtime()))
        traceback.print_exc()

if __name__ == '__main__':
    try:
        Logger.log("statistic scheduler start")
        scheduler.start()
        # HouNiaoScript().download_goods()
        Logger.log("statistic scheduler start success")
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        Logger.log("statistic scheduler start-up fail")



