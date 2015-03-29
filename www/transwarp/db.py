#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'zhan'

'''
Database operation module
'''
import  time, uuid, functools, threading, logging

#自定义的类，比dict多了一个初始化的功能
class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)    #调用父类的初始化函数
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"Dict' object has no attribute '%s'" % key)
    
    def __setattr__(self, key, value):
        self[key] = value

#返回一个唯一的ID号
def next_id(t=None):
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

#打印日志消息
def _profiling(start, sql=''):
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING][DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING][DB] %s: %s' % (t, sql))

class DBError(Exception):
    pass

class MultiColumsError(DBError):
    pass

#连接类，存储实际的连接，该连接由engine创建
class _LasyConnection(object):
    def __init__(self):
        self.connection = None
    
    def cursor(self):
        if self.connection is None:
            connection = engine.connect()       #engine.connect()创建实际的数据库连接
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()

#线程局部类，存储连接和事务信息，连接存储的是一个_LasyConnection对象
class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0
    
    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None
    
    def cursor(self):
        return self.connection.cursor()

_db_ctx = _DbCtx()  #线程局部对象


engine = None   #存放实际连接的变量，_LasyConnection从该变量中读取连接信息

#封装的一个连接类，engine就是这个类型
class _Engine(object):

    def __init__(self, connect):
        self._connect = connect

    def connect(self):              #实际创建数据库连接的函数
        return self._connect()

#给engine赋值，本质上说就是创建一个连接
def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')

    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda:mysql.connector.connect(**params))  #创建一个数据库连接
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


#操作_db_ctx变量的类
class _ConnectionCtx(object):

    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self
    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

#返回_ConnectionCtx类对象
def connection():
    return _ConnectionCtx()

#上面代码用于创建一个数据库连接
#实际连接存放在engine变量中，_db_ctx变量会存储该连接
#不太明白_LasyConnection的作用是什么
#两个变量是核心


#该函数的作用就是一个装饰器，实际调用的函数是_wrapper
#本质上就是在实际的函数前加上with _ConnectionCtx():
#功能就是执行实际函数之前初始化_db_ctx变量，之后释放_db_ctx变量
def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper


#和_ConnectionCtx的区别就是加上了事务处理
class _TransactionCtx(object):
    
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info('begin transaction...' if _db_ctx.transactions == 1 else 'join current transaction...')

    def __exit__(self, exctype,excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()
    
    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed, try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')

def transaction():
    return _TransactionCtx()

#装饰器，在调用实际函数之前会调用_TransactionCtx的__enter__
#在调用函数之后会调用_TransactionCtx的__exit___，完成对
#_db_ctx变量的初始化和释放操作
#目前还没用到这个函数
def with_transaction():
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper



#执行select SQL语句，first为True，只返回一条结果，如果为False，返回全部结果
def _select(sql, first, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()  #fetchone只返回一条结果
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]  #fetchall，返回所有结果
    finally:
        if cursor:
            cursor.close()

#只返回一行结果的select函数
@with_connection
def select_one(sql, *args):
    return _select(sql, True, *args) 

#
@with_connection
def select_int(sql, *args):
    d = _select(sql, True, *args)
    if len(d) != 1:
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]

#返回所有结果的select函数
@with_connection
def select(sql, *args):
    return _select(sql, False, *args)


#执行插入和更新SQL语句操作，并执行commit操作
@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()
#执行insert操作
def insert(table, **kw):
    cols, args = zip(*kw.iteritems())
    sql = 'insert into %s (%s) values (%s)' % (table, ','.join([' %s ' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))  #把insert操作转换成SQL语句
    return _update(sql, *args)

def update(sql, *args):
    return _update(sql, *args)

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('root', 'password', 'test')
    update('drop table if exists user')
    update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()
