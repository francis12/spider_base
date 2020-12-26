# -*- coding: utf-8 -*-
import hashlib
import os
import traceback
import json
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup

from script.houniao.xxkucun_bo import *
from script.houniao.logging_utils import Logger
from script.houniao.xkucun_common import XKCCommonParam


class XXKuCunScript():

    log_path = 'xxkucun_log'
    base_dir = os.path.dirname(__file__)
    base_url = 'http://www.houniao.hk'
    login_url = base_url + '/home/User/login'
    cookie_ts_key = 'Hm_lpvt_79b5f26a1df74b42929511d91ac22f32'
    download_path = os.path.join(XKCCommonParam.script_download_path, time.strftime("%Y%m%d%H%M%S", time.localtime()))

    def request_get_proxy(self, url, params=None):
        Logger.log('request get:' + url, this_dir=self.log_path)
        Logger.log(params)
        response = None
        retry_cnt = 1
        while True:
            response = requests.get(url=url, params=params, headers=self.get_now_header(), timeout= 60*5, verify=False)
            if response.status_code == 200:
                break
            else:
                retry_cnt = retry_cnt+1
                if retry_cnt > 10:
                    Logger.log('%s 重试超过10次，跳出'%(url), this_dir=self.log_path)
                    break
                Logger.log('%s请求失败：%d,重试第%d次'% (url, response.status_code, retry_cnt))
                time.sleep(5)
        return response

    def get_signature(self, src_id):
        md5 = hashlib.md5()
        md5.update(src_id.encode('utf-8'))
        return md5.hexdigest()

    def get_now_header(self):
        header = {
            "version": "2.9",
            "appkey": "98aqnb",
            "timestamp": "1608101674",
            "sign": "4ec64958698e0e3929576cc2033dc905",
            "user-agent": "okhttp/4.7.2"
        }
        # banner
        timestamp = int(time.time())
        sign_source = 'appkey=98aqnb&appsecret=BAD60AB6-F6D7-4F2F-9711-5EEF15287D4E&timestamp=' + str(timestamp)
        sign = self.get_signature(sign_source)
        header['timestamp'] = str(timestamp)
        header['sign'] = sign
        return header

    def download_goods(self):
        session = DBUtil.get_session()
        citys = session.execute('select * from t_city')
        for city in citys:
            self.download_city_goods(city)
        session.close()

    def download_city_goods(self, city):
        city_id = city[1]
        city_name = city[2]
        Logger.log('开始下载城市：'+ city_name, this_dir=self.log_path)

        banner_url = 'https://api.xxkucun.com/v1/product/banner/list?ver=1&city_id='+ str(city_id)
        banner_res = self.request_get_proxy(banner_url).json()

        # 下载区域
        Logger.log('开始下载区域， 条目基础数据', this_dir=self.log_path)
        district_url = 'https://api.xxkucun.com/v1/common/district/list?city_id='+ str(city_id)
        district_res = self.request_get_proxy(district_url).json()
        if 0 == district_res['err_code']:
            for district_data in district_res['data']:
                if DistrictDao.select_by_district_id(district_data['ID']) is None:
                    district_bo = DistrictBO()
                    district_bo.city_id = city_id
                    district_bo.district_id = district_data['ID']
                    district_bo.district_name = district_data['Name']
                    DistrictDao.insert(district_bo)

        catetory_url = 'https://api.xxkucun.com/v1/product/category/list?num=8&city_id=' + str(city_id)
        catetory_res = self.request_get_proxy(catetory_url).json()
        if 0 == catetory_res['err_code']:
            for catetory_data in catetory_res['data']:
                db_category = CategoryDao.select_by_city_id_categoryid(city_id, catetory_data['ID'])
                if db_category is None:
                    category_bo = CategoryBo()
                    category_bo.city_id = city_id
                    category_bo.level = 0
                    category_bo.category_id = catetory_data['ID']
                    category_bo.category_name = catetory_data['Name']
                    category_id = CategoryDao.insert(category_bo)
                else:
                    category_id = db_category.pid
                child_category_url = 'https://api.xxkucun.com/v1/product/catechild/list?category_id=%d&city_id=%d' % (catetory_data['ID'], city_id)
                child_catetory_res = self.request_get_proxy(child_category_url).json()
                if 0 == child_catetory_res['err_code']:
                    for catetory_data in child_catetory_res['data']:
                        db_category = CategoryDao.select_by_city_id_categoryid(city_id, catetory_data['ID'])
                        if db_category is None:
                            category_bo = CategoryBo()
                            category_bo.city_id = city_id
                            category_bo.level = 1
                            category_bo.parent_category_pid = category_id
                            category_bo.category_id = catetory_data['ID']
                            category_bo.category_name = catetory_data['Name']
                            CategoryDao.insert(category_bo)

        # 条目下商品
        Logger.log('开始下载条目商品数据', this_dir=self.log_path)
        distircts = DistrictDao.select_by_city_id(city_id)
        categories = CategoryDao.select_by_city_id_level(city_id, 2)
        for district in distircts:
            for category in categories:
                page_index = 1
                while True:
                    category_district_search_url = 'https://api.xxkucun.com/v1/product/search/by/category?page_index=%d&district_id=%d&px=0&category_id=%d&type=0&lng=103.56358166666665&lat=33.00125&actiontype=&city_id=%d' % (page_index, district.district_id,category.category_id,city_id )
                    category_district_search_res = self.request_get_proxy(category_district_search_url).json()
                    if 0 == category_district_search_res['err_code']:
                        if len(category_district_search_res['data']) == 0:
                            break
                        for rec_data in category_district_search_res['data']:
                            product_list_bo = ProductListBO()
                            product_list_bo.city_id = city_id
                            product_list_bo.district_id = district.district_id
                            product_list_bo.category_pid = category.pid
                            product_list_bo.product_id = rec_data['ID']
                            product_list_bo.name = rec_data['Name']
                            product_list_bo.img_url = rec_data['Img']
                            self.download_pic_url(rec_data['Img'], rec_data['ID'])
                            product_list_bo.brand_name = rec_data['BrandName']
                            product_list_bo.price = rec_data['Price']
                            product_list_bo.market_price = rec_data['MarketPrice']
                            product_list_bo.sale_qty = rec_data['SaleQty']
                            product_list_bo.total_qty = rec_data['TotalQty']
                            product_list_bo.discount = rec_data['Discount']
                            product_list_bo.commission = rec_data['Commission']
                            product_list_bo.group_type = 'SEARCH'
                            product_list_bo.sale_status = rec_data.get('SaleStatus', None)
                            ProductListDao.insert(product_list_bo)
                            # self.down_good_detail(rec_data['ID'], city_id)
                    else:
                        break
                    page_index =  page_index + 1
        Logger.log('开始下载BANNER数据', this_dir=self.log_path)
        banner_url = 'https://api.xxkucun.com/v1/product/banner/list?ver=1&city_id='+ str(city_id)
        banner_res = self.request_get_proxy(banner_url).json()
        if 0 == banner_res['err_code']:
            for banner_data in banner_res['data']:
                product_list_bo = ProductListBO()
                product_list_bo.city_id = city_id
                product_list_bo.product_id = banner_data['ProdID']
                product_list_bo.img_url = banner_data['Img']
                self.download_pic_url(banner_data['Img'])
                product_list_bo.group_type = banner_data['type']
                product_list_bo.sale_status = banner_data.get('SaleStatus', None)
                ProductListDao.insert(product_list_bo)
        Logger.log('开始下载今日推荐', this_dir=self.log_path)
        rec_url = 'https://api.xxkucun.com/v1/product/today/rec/list?lng=103.56358166666665&lat=33.00125&city_id='+ str(city_id)
        rec_res = self.request_get_proxy(rec_url).json()
        if 0 == rec_res['err_code']:
            for rec_data in rec_res['data']:
                product_list_bo = ProductListBO()
                product_list_bo.city_id = city_id
                product_list_bo.product_id = rec_data['ID']
                product_list_bo.name = rec_data['Name']
                product_list_bo.img_url = rec_data['Img']
                self.download_pic_url(rec_data['Img'], rec_data['ID'])
                product_list_bo.brand_name = rec_data['BrandName']
                product_list_bo.price = rec_data['Price']
                product_list_bo.market_price = rec_data['MarketPrice']
                product_list_bo.commission = rec_data['Commission']
                product_list_bo.pay_count = rec_data['PayCount']
                product_list_bo.group_type = 'REC'
                product_list_bo.sale_status = rec_data.get('SaleStatus', None)
                ProductListDao.insert(product_list_bo)
                self.down_good_detail(rec_data['ID'], city_id)


        Logger.log('开始下载即将下线', this_dir=self.log_path)
        down_url = 'https://api.xxkucun.com/v1/product/pro/GetDownLineList?user_id=null&lng=103.56358166666665&lat=33.00125&city_id='+ str(city_id)
        down_res = self.request_get_proxy(down_url).json()
        if 0 == down_res['err_code']:
            for rec_data in down_res['data']:
                product_list_bo = ProductListBO()
                product_list_bo.city_id = city_id
                product_list_bo.product_id = rec_data['ID']
                product_list_bo.name = rec_data['Name']
                product_list_bo.img_url = rec_data['Img']
                self.download_pic_url(rec_data['Img'], rec_data['ID'])
                product_list_bo.brand_name = rec_data['BrandName']
                product_list_bo.price = rec_data['Price']
                product_list_bo.market_price = rec_data['MarketPrice']
                product_list_bo.sale_qty = rec_data['SaleQty']
                product_list_bo.total_qty = rec_data['TotalQty']
                product_list_bo.discount = rec_data['Discount']
                product_list_bo.commission = rec_data['Commission']
                product_list_bo.group_type = 'DOWN'
                product_list_bo.sale_status = rec_data.get('SaleStatus', None)
                ProductListDao.insert(product_list_bo)
                self.down_good_detail(rec_data['ID'], city_id)

        Logger.log('开始下载爆款推荐', this_dir=self.log_path)
        hot_url = 'https://api.xxkucun.com/v1/product/hot/list?page_index=1&lng=103.56358166666665&lat=33.00125&city_id='+ str(city_id)
        hot_res = self.request_get_proxy(hot_url).json()
        if 0 == hot_res['err_code']:
            for rec_data in hot_res['data']:
                product_list_bo = ProductListBO()
                product_list_bo.city_id = city_id
                product_list_bo.product_id = rec_data['ID']
                product_list_bo.name = rec_data['Name']
                product_list_bo.img_url = rec_data['Img']
                self.download_pic_url(rec_data['Img'], rec_data['ID'])
                product_list_bo.brand_name = rec_data['BrandName']
                product_list_bo.price = rec_data['Price']
                product_list_bo.market_price = rec_data['MarketPrice']
                product_list_bo.sale_qty = rec_data['SaleQty']
                product_list_bo.total_qty = rec_data['TotalQty']
                product_list_bo.discount = rec_data['Discount']
                product_list_bo.commission = rec_data['Commission']
                product_list_bo.group_type = 'HOT'
                product_list_bo.sale_status = rec_data.get('SaleStatus', None)
                ProductListDao.insert(product_list_bo)
                self.down_good_detail(rec_data['ID'], city_id)

        Logger.log('开始下载专栏', this_dir=self.log_path)
        category_url = 'https://api.xxkucun.com/v1/product/GetListArrayByCategory?lng=33.00125&lat=103.56358166666665&city_id='+ str(city_id)
        category_res = self.request_get_proxy(category_url).json()
        if 0 == category_res['err_code']:
            for rec_data in category_res['data']:
                type_name = rec_data['Name']
                db_category = CategoryDao.select_by_city_id_categoryid(city_id, rec_data['ID'])
                # category_bo = CategoryBo()
                # category_bo.category_name = type_name
                # category_bo.city_id = city_id
                # category_bo.level = 99
                # db_category = CategoryDao.select_by_city_id_level_name(city_id, 99, type_name)
                # if db_category is None:
                #     category_pid = CategoryDao.insert(category_bo)
                # else:
                #     category_pid = db_category.pid
                for item in rec_data['List']:
                    product_list_bo = ProductListBO()
                    product_list_bo.city_id = city_id
                    product_list_bo.product_id = item['ID']
                    product_list_bo.name = item['Name']
                    product_list_bo.img_url = item['Img']
                    self.download_pic_url(item['Img'], item['ID'])
                    product_list_bo.brand_name = item['BrandName']
                    product_list_bo.price = item['Price']
                    product_list_bo.market_price = item['MarketPrice']
                    product_list_bo.sale_qty = item['SaleQty']
                    product_list_bo.total_qty = item['TotalQty']
                    product_list_bo.discount = item['Discount']
                    product_list_bo.commission = item['Commission']
                    product_list_bo.group_type = 'CATEGORY_' + type_name
                    product_list_bo.category_pid = db_category.pid
                    product_list_bo.sale_status = rec_data.get('SaleStatus', None)
                    ProductListDao.insert(product_list_bo)
                    self.down_good_detail(item['ID'], city_id)

    def down_good_detail(self, product_id, city_id):
        detail_url = 'https://api.xxkucun.com/v1/product/detail/get?prod_id=%d&lng=103.56358166666665&lat=33.00125&city_id=%d' %(product_id, city_id)
        detail_res = self.request_get_proxy(detail_url).json()
        if 0 == detail_res['err_code']:
            rec_data = detail_res['data']
            detail_bo = ProductDetailBO()
            detail_bo.product_id = rec_data['ID']
            detail_bo.name = rec_data['Name']
            detail_bo.imgs = str(rec_data['Imgs'])
            num = 0
            for img in rec_data['Imgs']:
                self.download_pic_url(img, file_name=str(rec_data['ID']) + '_'+ str(num))
                num = num + 1
            detail_bo.price = rec_data['Price']
            detail_bo.market_price = rec_data['MarketPrice']
            detail_bo.brand_id = rec_data['BrandID']
            detail_bo.brand_name = rec_data['BrandName']
            detail_bo.brand_logo = rec_data['BrandLogo']
            detail_bo.fxwa = rec_data['FXWA']
            detail_bo.commission = rec_data['Commission']
            detail_bo.max_commission = rec_data['MaxCommission']
            detail_bo.sale_qty = rec_data['SaleQty']
            detail_bo.total_qty = rec_data['AvaQty']
            detail_bo.sale_time = rec_data['SaleTime']
            detail_bo.offline_time = rec_data['OfflineTime']
            detail_bo.total_seconds = rec_data['TotalSeconds']
            detail_bo.stores = json.dumps(rec_data['Stores'], ensure_ascii=False)
            # detail_bo.condition = rec_data['Condition']
            # detail_bo.remark = rec_data['Remark']
            # detail_bo.setmeal = rec_data['Setmeal']
            detail_bo.condition_url = rec_data['ConditionUrl']
            detail_bo.remark_url = rec_data['RemarkUrl']
            detail_bo.setmeal_url = rec_data['SetmealUrl']
            ProductDetailDao.insert(detail_bo)



    def download_pic_url(self, img_url=None, file_name=None):
        #todo 去除url中的参数串
        try:
            if '?' in img_url:
                img_url = img_url[:img_url.index('?')]
            split_file_name = img_url.split('/')[-1]
            path_arr = img_url.split('/')[3:-1]
            if file_name is None:
                file_name = split_file_name
            else:
                file_name = str(file_name) + os.path.splitext(split_file_name)[-1]
            if img_url.startswith('/'):
                img_url = img_url[1:]
            download_path = os.path.join(XKCCommonParam.script_download_path,*path_arr)
            if not os.path.exists(download_path):
                os.makedirs(download_path)
            r = requests.get(img_url, stream=True, timeout=60*5)
            abs_file_path = os.path.join(download_path, file_name)
            # print('保存图片：' + abs_file_path)
            with open(abs_file_path, 'wb') as f:
                f.write(r.content)
            return True
        except Exception as err:
            traceback.print_exc()
            Logger.log('img %s download err' % img_url, this_dir=self.log_path)
            return False

    def get_code_from_href(self, href):
        if href is None:
            return None
        return href[href.index('itemSku=')+len('itemSku='):]

scheduler = BlockingScheduler()
@scheduler.scheduled_job("cron", day_of_week='*', hour=XKCCommonParam.job_hour, minute=XKCCommonParam.job_min, second='00')
def rebate():
    script = XXKuCunScript()
    try:
        Logger.log('spider task start', this_dir=script.log_path)
        script.download_goods()
        Logger.log('Spider task end', this_dir=script.log_path)
    except BaseException as err:
        Logger.log('Spider task failed: ' + time.strftime('%Y-%m-%d', time.localtime()), this_dir=script.log_path)
        traceback.print_exc()
    # Logger.log("statistic scheduler execute success" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == '__main__':
    try:
        Logger.log("statistic scheduler start", this_dir=XXKuCunScript.log_path)
        DBUtil.param = XKCCommonParam
        # scheduler.start()
        XXKuCunScript().download_goods()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        Logger.log("statistic scheduler start-up fail", this_dir=XXKuCunScript.log_path)



