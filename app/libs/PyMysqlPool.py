#!/usr/bin/env python
# coding:utf-8

from gevent import monkey

monkey.patch_all()

import queue
import threading

from .Utility import *


class PyMysqlPool(object):
    """
    python 初始化类最先调用__new__(cls,*args,**kargs)方法，
            返回实例给__init__(self)做后续的初始化工作
        如果要实现单例模式需要手写__new__方法
    """

    def __init__(self, config, initial_size=1, max_size=20):
        """
        :param config: 数据库连接信息
        :param initial_size: 初始化连接数
        :param max_size: 最大连接数
        :return: 连接池对象
        """
        self.config = config
        self.initial_size = initial_size
        self.__active_size = initial_size
        self.max_size = max_size
        self.__mutex = threading.Lock()
        self.pool = queue.Queue(max_size)
        for i in range(initial_size):
            try:
                conn = pymysql.connect(**config)
                self.pool.put(conn)
            except Exception as e:
                raise IOError(e)

    def __del__(self):
        """
        对象回收时会调用__del__方法
        """
        self.__close_all_conn()

    def __get(self):
        """
        __get包装，连接不够用时动态创建一个新连接
        """
        # 该mutex.acquire()如果放到if里面
        # 当并发线程数大于数据库连接数时，由于没有设置优先执行最长超时机制
        # 总是导致后来线程一直得不到连接，一直处于阻塞状态
        # 相当于大量用户参与秒杀，总是有些后来的用户卡死直到提示秒杀失败(如果是设置了超时)
        # 故扩大范围在外面加锁
        # 在整个__get()范围加mutex锁会使线程均分配到CPU时间片，即得到执行
        # 该问题有待深入研究
        self.__mutex.acquire()
        if self.pool.empty() and self.__active_size < self.pool.maxsize:
            conn = pymysql.connect(**self.config)
            self.__active_size += 1
        else:
            conn = self.pool.get()
        self.__mutex.release()
        return conn

    def __put(self, connect):
        if self.pool.full():
            connect.close()
        else:
            self.pool.put(connect)

    def get_conn_cursor(self, auto_commit=True, dict_cursor=False):
        try:
            # 当前连接池如果没有可用连接就阻塞在这里
            connect = self.__get()
            if auto_commit is True:
                connect.autocommit(True)
            else:
                connect.autocommit(False)
            if dict_cursor is True:
                cursor = connect.cursor(cursor=pymysql.cursors.DictCursor)
            else:
                cursor = connect.cursor(cursor=pymysql.cursors.Cursor)
            return connect, cursor
        except Exception as e:
            log(e)
            cursor.close()
            self.__put(connect)
            return None

    def query_scalar(self, sql):
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=True, dict_cursor=False)
            cursor.execute(sql)
            row = cursor.fetchone()
            scalar = row[0]
            return scalar
        except Exception as e:
            log("query_scalar error:{e}, sql = '{sql}'".format(e=e, sql=sql))
            return None
        finally:
            cursor.close()
            self.__put(connect)

    def query_column(self, sql):
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=True, dict_cursor=False)
            cursor.execute(sql)
            rows = cursor.fetchall()
            column = [(row[0]) for row in rows]
            return column
        except Exception as e:
            log("query_column error:{e}, sql = '{sql}'".format(e=e, sql=sql))
            return None
        finally:
            cursor.close()
            self.__put(connect)

    def query_one(self, sql):
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=True, dict_cursor=False)
            cursor.execute(sql)
            return cursor.fetchone()
        except Exception as e:
            log("query_one error:{e}, sql = '{sql}'".format(e=e, sql=sql))
            return None
        finally:
            cursor.close()
            self.__put(connect)

    def query_one_dict(self, sql):
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=True, dict_cursor=True)
            cursor.execute(sql)
            return cursor.fetchone()
        except Exception as e:
            log("query_one_dict error:{e}, sql = '{sql}'".format(e=e, sql=sql))
            return None
        finally:
            cursor.close()
            self.__put(connect)

    def query_all(self, sql):
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=True, dict_cursor=False)
            cursor.execute(sql)
            return list(cursor.fetchall())
        except Exception as e:
            log("query_all error:{e}, sql = '{sql}'".format(e=e, sql=sql))
            return None
        finally:
            cursor.close()
            self.__put(connect)

    def query_all_dict(self, sql):
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=True, dict_cursor=True)
            cursor.execute(sql)
            return list(cursor.fetchall())
        except Exception as e:
            log("query_all_dict error:{e}, sql = '{sql}'".format(e=e, sql=sql))
            return None
        finally:
            cursor.close()
            self.__put(connect)

    def execute(self, sql):
        rows_affected = 0
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=False, dict_cursor=False)
            rows_affected = cursor.execute(sql)
            connect.commit()
        except Exception as e:
            connect.rollback()
            log("update error:{e}, sql = '{sql}'".format(e=e, sql=sql))
            return None
        finally:
            cursor.close()
            self.__put(connect)
            return rows_affected

    def insert(self, sql, items):
        '''
        :param sql: str e.g. insert into tablename (id,name) values (%s,%s)
        :param items: list or tuple e.g. [(1,'a'),(2,'b')]
        :return:
        '''
        rows_affected = 0
        try:
            connect, cursor = self.get_conn_cursor(auto_commit=False, dict_cursor=False)
            rows_affected = cursor.executemany(sql, items)
            connect.commit()
        except Exception as e:
            connect.rollback()
            log("insert error:{e}, sql = '{sql}' values = {values}"
                .format(e=e, sql=sql, values=items))
            return None
        finally:
            cursor.close()
            self.__put(connect)
            return rows_affected

    def __close_all_conn(self):
        for i in range(self.pool.qsize()):
            if self.pool.empty():
                pass
            else:
                self.pool.get().close()
