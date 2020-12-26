import datetime
import time
from script.houniao.houniao_util import DBUtil, BeanUtil
from sqlalchemy import Column, String
from sqlalchemy.types import INTEGER,FLOAT, TEXT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()



class DistrictBO(Base):
    # 表的名字:
    __tablename__ = 't_district'

    # 表的结构:
    pid = Column(INTEGER, primary_key=True, autoincrement=True)
    city_id = Column(INTEGER)
    district_id = Column(INTEGER)
    district_name = Column(String(255))
    district_url = Column(String(255))


class DistrictDao:
    @staticmethod
    def insert(item: DistrictBO):
        session = DBUtil.get_session()
        # 插入
        session.add(item)
        session.commit()
        pid = item.pid
        session.close()
        return pid

    @staticmethod
    def select_all():
        session = DBUtil.get_session()
        res = session.query(DistrictBO)
        session.close()
        return res

    @staticmethod
    def select_by_district_id(district_id):
        session = DBUtil.get_session()
        res = session.query(DistrictBO).filter(DistrictBO.district_id ==district_id).first()
        session.close()
        return res

    @staticmethod
    def select_by_city_id(city_id):
        session = DBUtil.get_session()
        res = session.query(DistrictBO).filter(DistrictBO.city_id ==city_id).all()
        session.close()
        return res

    @staticmethod
    def update_detail(room: DistrictBO):
        session = DBUtil.get_session()
        item = session.query(DistrictBO).filter(DistrictBO.pid == room.pid).first()
        BeanUtil.copy_obj_properties(room, item)
        session.commit()
        session.close()



class CategoryBo(Base):
    # 表的名字:
    __tablename__ = 't_category'

    # 表的结构:
    pid = Column(INTEGER, primary_key=True, autoincrement=True)
    city_id = Column(INTEGER)
    category_id = Column(INTEGER)
    parent_category_pid = Column(INTEGER)
    category_name = Column(String(255))
    level = Column(String(255))


class CategoryDao:
    @staticmethod
    def insert(item: CategoryBo):
        session = DBUtil.get_session()
        # 插入
        session.add(item)
        session.commit()
        pid = item.pid
        session.close()
        return pid

    @staticmethod
    def select_all():
        session = DBUtil.get_session()
        res = session.query(CategoryBo)
        session.close()
        return res

    @staticmethod
    def select_by_city_id_level(city_id, level):
        session = DBUtil.get_session()
        res = session.query(CategoryBo).filter(CategoryBo.city_id ==city_id).filter(CategoryBo.level ==level).all()
        session.close()
        return res

    @staticmethod
    def select_by_city_id_level_name(city_id, level, name):
        session = DBUtil.get_session()
        res = session.query(CategoryBo).filter(CategoryBo.city_id ==city_id).filter(CategoryBo.level ==level).filter(CategoryBo.category_name ==name).first()
        session.close()
        return res

    @staticmethod
    def select_by_city_id_categoryid(city_id, cateory_id):
        session = DBUtil.get_session()
        res = session.query(CategoryBo).filter(CategoryBo.city_id ==city_id).filter(CategoryBo.category_id ==cateory_id).first()
        session.close()
        return res

    @staticmethod
    def update_detail(room: CategoryBo):
        session = DBUtil.get_session()
        item = session.query(CategoryBo).filter(CategoryBo.pid == room.pid).first()
        BeanUtil.copy_obj_properties(room, item)
        session.commit()
        session.close()


class ProductListBO(Base):
    # 表的名字:
    __tablename__ = 't_product_list'

    # 表的结构:
    pid = Column(INTEGER, primary_key=True, autoincrement=True)
    city_id = Column(INTEGER)
    product_id = Column(INTEGER)
    category_pid = Column(INTEGER)
    district_id = Column(INTEGER)
    img_url = Column(String(255))
    group_type = Column(String(255))
    name = Column(String(255))
    brand_name = Column(String(255))
    price = Column(String(255))
    market_price = Column(String(255))
    sale_time = Column(String(255))
    discount = Column(FLOAT)
    commission = Column(FLOAT)
    sale_qty = Column(INTEGER)
    total_qty = Column(INTEGER)
    pay_count = Column(INTEGER)
    total_seconds = Column(INTEGER)
    distance = Column(INTEGER)
    time_format = Column(String(255))
    sale_status = Column(String(255))
    create_time = Column(String(255))

    def __init__(self,
                 city_id=None,
                 product_id=None,
                 img_url=None,
                 group_type=None,
                 name=None,
                 brand_name=None,
                 price=None,
                 market_price=None,
                 sale_time=None,
                 discount=None,
                 commission=None,
                 sale_qty=None,
                 total_qty=None,
                 pay_count=None,
                 total_seconds=None,
                 distance=None,
                 time_format=None,
                 create_time=None):
        self.city_id = city_id
        self.product_id = product_id
        self.img_url = img_url
        self.group_type = group_type
        self.name = name
        self.brand_name = brand_name
        self.price = price
        self.market_price = market_price
        self.sale_time = sale_time
        self.discount = discount
        self.commission = commission
        self.sale_qty = sale_qty
        self.total_qty = total_qty
        self.pay_count = pay_count
        self.total_seconds = total_seconds
        self.distance = distance
        self.time_format = time_format
        self.create_time = datetime.datetime.now()


class ProductListDao:
    @staticmethod
    def insert(item: ProductListBO):
        session = DBUtil.get_session()
        # 插入
        session.add(item)
        session.commit()
        pid = item.pid
        session.close()
        return pid

    @staticmethod
    def select_all():
        session = DBUtil.get_session()
        res = session.query(ProductListBO)
        session.close()
        return res

    @staticmethod
    def update_detail(room: ProductListBO):
        session = DBUtil.get_session()
        item = session.query(ProductListBO).filter(ProductListBO.pid == room.pid).first()
        BeanUtil.copy_obj_properties(room, item)
        session.commit()
        session.close()


class ProductDetailBO(Base):
    # 表的名字:
    __tablename__ = 't_product_detail'

    # 表的结构:
    pid = Column(INTEGER, primary_key=True, autoincrement=True)
    product_id = Column(INTEGER)
    name = Column(String(255))
    imgs = Column(String(1024))
    price = Column(String(255))
    market_price = Column(String(255))
    brand_id = Column(INTEGER)
    brand_name = Column(String(255))
    brand_logo = Column(String(255))
    fxwa = Column(String(255))
    commission = Column(FLOAT)
    max_commission = Column(FLOAT)
    sale_qty = Column(INTEGER)
    total_qty = Column(INTEGER)
    sale_time = Column(String(255))
    offline_time = Column(String(255))
    total_seconds = Column(INTEGER)
    book_way = Column(String(255))
    stores = Column(TEXT)
    condition = Column(TEXT)
    remark = Column(TEXT)
    setmeal = Column(TEXT)
    condition_url = Column(String(255))
    remark_url = Column(String(255))
    setmeal_url = Column(String(255))
    create_time = Column(String(255))

    def __init__(self,
                 product_id=None,
                 name=None,
                 imgs=None,
                 price=None,
                 market_price=None,
                 brand_id=None,
                 brand_name=None,
                 brand_logo=None,
                 fxwa=None,
                 commission=None,
                 max_commission=None,
                 sale_qty=None,
                 total_qty=None,
                 sale_time=None,
                 offline_time=None,
                 total_seconds=None,
                 book_way=None,
                 stores=None,
                 condition=None,
                 remark=None,
                 setmeal=None,
                 condition_url=None,
                 remark_url=None,
                 setmeal_url=None,
                 create_time=None):
        self.product_id = product_id
        self.name = name
        self.imgs = imgs
        self.price = price
        self.price = price
        self.market_price = market_price
        self.brand_id = brand_id
        self.brand_name = brand_name
        self.brand_logo = brand_logo
        self.fxwa = fxwa
        self.commission = commission
        self.max_commission = max_commission
        self.sale_qty = sale_qty
        self.total_qty = total_qty
        self.sale_time = sale_time
        self.offline_time = offline_time
        self.total_seconds = total_seconds
        self.book_way = book_way
        self.stores = stores
        self.condition = condition
        self.remark = remark
        self.setmeal = setmeal
        self.condition_url = condition_url
        self.remark_url = remark_url
        self.setmeal_url = setmeal_url
        self.create_time = datetime.datetime.now()


class ProductDetailDao:
    @staticmethod
    def insert(item: ProductDetailBO):
        session = DBUtil.get_session()
        # 插入
        session.add(item)
        session.commit()
        pid = item.pid
        session.close()
        return pid

    @staticmethod
    def select_all():
        session = DBUtil.get_session()
        res = session.query(ProductDetailBO)
        session.close()
        return res

    @staticmethod
    def update_detail(room: ProductDetailBO):
        session = DBUtil.get_session()
        item = session.query(ProductDetailBO).filter(ProductDetailBO.pid == room.pid).first()
        BeanUtil.copy_obj_properties(room, item)
        session.commit()
        session.close()
