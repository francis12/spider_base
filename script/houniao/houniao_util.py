from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from script.houniao.common import HNCommonParam


class DBUtil:
    bp_pool = None
    param = HNCommonParam

    @classmethod
    def get_session(cls):
        # ��ʼ�����ݿ�����:
        cls.bp_pool = create_engine(cls.param.database_url, pool_size=cls.param.database_pool_size,
                                    max_overflow=cls.param.database_max_overflow,
                                    pool_recycle=3600 * 6) if cls.bp_pool is None else cls.bp_pool
        # ����DBSession����
        DBSession = sessionmaker(bind=cls.bp_pool)
        session = DBSession()
        return session


class BeanUtil:
    @staticmethod
    def copy_obj_properties(_from=None, to=None):
        fields = dir(_from)
        for field in fields:
            if not (field.startswith("__") or field.startswith("_")):
                if getattr(_from, field) is not None:
                    if hasattr(to, field):
                        # print(field)
                        setattr(to, field, getattr(_from, field))
                        # print(getattr(to, field))
        return to

    @staticmethod
    def copy_dict_properties(_from=None, to=None):
        for key in _from:
            if _from[key] is not None:
                to[key] = _from[key]
        return to

    @staticmethod
    def item_to_bo(item, bo_class):
        bo_instance = bo_class()
        items = dict(item.items())
        for key in items:
            if hasattr(bo_class, key):
                setattr(bo_instance, key, items[key])
        return bo_instance
