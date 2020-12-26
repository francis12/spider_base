import datetime
import time
from script.houniao.houniao_util import DBUtil, BeanUtil
from sqlalchemy import Column, String
from sqlalchemy.types import INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ActivtiyBO(Base):
    # 表的名字:
    __tablename__ = 't_activity'

    # 表的结构:
    pid = Column(INTEGER, primary_key=True, autoincrement=True)
    activity_code = Column(String(255))
    name = Column(String(255))
    type = Column(String(255))
    url = Column(String(255))
    pic_url = Column(String(255))
    desc = Column(String(255))
    day = Column(String(255))
    create_time = Column(String(255))

    def __init__(self,
                 activity_code=None,
                 name=None,
                 type=None,
                 url=None,
                 pic_url=None,
                 day=None,
                 desc=None):
        self.activity_code = activity_code
        self.name = name
        self.type = type
        self.url = url
        self.pic_url = pic_url
        self.day = time.strftime("%Y-%m-%d", time.localtime())
        self.desc = desc
        self.create_time = datetime.datetime.now()


class ActivtiyDao:
    @staticmethod
    def insert(item: ActivtiyBO):
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
        res = session.query(ActivtiyBO)
        session.close()
        return res

    @staticmethod
    def update_room_detail(room: ActivtiyBO):
        session = DBUtil.get_session()
        item = session.query(ActivtiyBO).filter(ActivtiyBO.pid == room.pid).first()
        BeanUtil.copy_obj_properties(room, item)
        session.commit()
        session.close()


class GoodBo(Base):
    # 表的名字:
    __tablename__ = 't_good'

    # 表的结构:
    pid = Column(INTEGER, primary_key=True, autoincrement=True)
    name = Column(String(255))
    code = Column(String(255))
    price = Column(String(255))
    url = Column(String(255))
    sub_title = Column(String(255))
    sub_sub_title = Column(String(255))
    activity_code = Column(String(255))
    activity_pid = Column(INTEGER)
    day = Column(String(255))
    pic_url = Column(String(255))
    create_time = Column(String(255))

    def __init__(self,
                 name=None,
                 code=None,
                 url=None,
                 sub_title=None,
                 sub_sub_title=None,
                 activity_code=None,
                 activity_pid=None,
                 price=None,
                 day=None,
                 pic_url=None):
        self.name = name
        self.code = code
        self.url = url
        self.pic_url = pic_url
        self.sub_title = sub_title
        self.sub_sub_title = sub_sub_title
        self.activity_code = activity_code
        self.activity_pid = activity_pid
        self.day = time.strftime("%Y-%m-%d", time.localtime())
        self.price = price
        self.create_time = datetime.datetime.now()


class GoodDao:
    @staticmethod
    def insert(item: GoodBo):
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
        res = session.query(GoodBo)
        session.close()
        return res

    @staticmethod
    def update_room_detail(room: GoodBo):
        session = DBUtil.get_session()
        item = session.query(GoodBo).filter(GoodBo.pid == room.pid).first()
        BeanUtil.copy_obj_properties(room, item)
        session.commit()
        session.close()
