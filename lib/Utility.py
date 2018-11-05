#!/usr/bin/env python
# coding:utf-8
import hashlib
import json
import math
import os
import random
import time

import jieba
import pymysql
from gensim import corpora, models, similarities


# 简单的测试一个字符串的MD5值
def get_str_md5(src):
    md5 = hashlib.md5()
    md5.update(src)
    return md5.hexdigest()


# 大文件的MD5值
def get_file_md5(filename):
    if not os.path.isfile(filename):
        return
    md5 = hashlib.md5()
    f = open(filename, 'rb')
    while True:
        b = f.read(1024 * 1024)
        if not b:
            break
        md5.update(b)
    f.close()
    return md5.hexdigest()


def get_filepath_sha1(filepath):
    with open(filepath, 'rb') as f:
        sha1 = hashlib.sha1()
        sha1.update(f.read())
        return sha1.hexdigest()


def get_filepath_md5(filepath):
    with open(filepath, 'rb') as f:
        md5 = hashlib.md5()
        md5.update(f.read())
        return md5.hexdigest()


# 控制台输出信息
def log(*info):
    print('[{time}] Info ['.format(time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())), )
    for msg in info:
        print(msg, )
    print(']')


# 随机暂停n秒
def random_sleep():
    sleep_secs = random.randint(3, 10)
    log('random sleep {sleep_secs}s'.format(sleep_secs=sleep_secs))
    time.sleep(sleep_secs)
    return sleep_secs


def get_cursor(config, dict_cursor=False, auto_commit=True):
    connect = pymysql.connect(**config)
    if auto_commit is True:
        connect.autocommit(True)
    else:
        connect.autocommit(False)
    if dict_cursor is True:
        return connect.cursor(cursor=pymysql.cursors.DictCursor)
    else:
        return connect.cursor(cursor=pymysql.cursors.Cursor)


def query_scalar(config, sql):
    cursor = get_cursor(config=config)
    cursor.execute(sql)
    row = cursor.fetchone()
    scalar = row[0]
    return scalar


def query_column(config, sql):
    cursor = get_cursor(config=config)
    cursor.execute(sql)
    rows = cursor.fetchall()
    column = [(row[0]) for row in rows]
    return column


def query_one(config, sql):
    cursor = get_cursor(config=config)
    cursor.execute(sql)
    return cursor.fetchone()


def query_one_dict(config, sql):
    cursor = get_cursor(config=config, dict_cursor=True)
    cursor.execute(sql)
    return cursor.fetchone()


def query_all(config, sql):
    cursor = get_cursor(config=config)
    cursor.execute(sql)
    return list(cursor.fetchall())


def query_all_dict(config, sql):
    cursor = get_cursor(config=config, dict_cursor=True)
    cursor.execute(sql)
    return list(cursor.fetchall())


def update(config, sql):
    connect = pymysql.connect(**config)
    connect.autocommit(False)
    rows_affected = 0
    try:
        with connect.cursor() as cursor:
            """
            如果autocommit设置为False，则会执行sql，但不提交更改。
            即：如果sql对表有更改操作，execute会返回成功影响的行数rows_affected，相当于尝试执行sql
                但由于autocommit为False，数据库并不会真正保存sql对数据的更改结果，
                直到执行connect.commit()命令，才会保存更改，常用在事务提交。
            """
            rows_affected = cursor.execute(sql)
        connect.commit()
    except Exception as e:
        connect.rollback()
        log("update error:{exception}, sql = '{sql}'".format(exception=e, sql=sql))
    finally:
        connect.close()
        return rows_affected


def insert(config, sql, items):
    connect = pymysql.connect(**config)
    # 更新数据的操作最好禁用自动提交
    connect.autocommit(False)
    rows_affected = 0
    try:
        with connect.cursor() as cursor:
            rows_affected = cursor.executemany(sql, items)
        connect.commit()
    except Exception as e:
        connect.rollback()
        log("insert error:{exception}, sql = '{sql}' values = {values}".format(exception=e, sql=sql, values=items))
    finally:
        connect.close()
        return rows_affected


def get_current_time_millis():
    return int(time.time() * 1000)


def chunk_list(array, size, is_chunk_number=False):
    array = list(array)
    list_size = len(array)
    if list_size == 0:
        raise Exception('ValueError: list_size={list_size}'.format(list_size=list_size))

    if is_chunk_number is True:
        size = math.ceil(list_size / float(math.ceil(size)))
    chunk_size = int(size)

    if chunk_size == 0:
        raise Exception('ValueError: chunk_size={chunk_size}'.format(chunk_size=chunk_size))

    result_list = list(zip(*[iter(array)] * chunk_size))

    mod = list_size % chunk_size
    if mod > 0:
        tail = tuple(array[-mod:])
        result_list.append(tail)
    return result_list


def print_json(obj=None):
    """
    use to print chinese with avoiding unicode
    :param obj: can be json.dumps()
    :return: None
    """
    print(json.dumps(obj, encoding="UTF-8", ensure_ascii=False))


def get_close_matches(word, possibilities, n=3, cutoff=0.01, cut_all=True, ret_index_and_ratio=False):
    """
    inspire from difflib.get_close_matches
    this method can use to comparing chinese string
    use jieba, a Chinese word segmentation module
    :param word: the source doc string
    :param possibilities: the target docs
    :param n: how many possibilities return
    :param cutoff: low than cutoff possibilities will drop
    :param cut_all: if cut_all is True then use jieba full model to segment chinese word
    :param ret_index_and_ratio: if this is true, return the origin possibilities index and ratio
    :return: list. ['apple','apply'] or [(3, 0.6, 'apple'), (1, 0.55, 'apply')]. the ratio between in [0, 1]
    """
    # 对possibilities进行分词
    all_possibilities_list = []
    for possibility in possibilities:
        # jieba.cut cut_all=True 全模式 能拆成独立词义的都拆开,不然很有可能没有匹配项
        possibilities_list = [cut_word for cut_word in jieba.cut(possibility, cut_all=cut_all)]
        remove_stop_word_possibilities_list = list()
        for cut_word in possibilities_list:
            if cut_word not in [u',', u'.', u'。', u'>', u'/']:
                remove_stop_word_possibilities_list.append(cut_word)
        all_possibilities_list.append(remove_stop_word_possibilities_list)

    # 对word进行分词
    word_list = [cut_word for cut_word in jieba.cut(word, cut_all=cut_all)]

    # 用possibilities分词结果制作语料库
    # step 1: 首先用dictionary方法获取词袋（bag-of-words)
    dictionary = corpora.Dictionary(all_possibilities_list)
    dictionary.keys()
    dictionary.token2id

    # step 2: 利用词袋制作语料库
    corpus = [dictionary.doc2bow(doc) for doc in all_possibilities_list]

    # 以下用同样的方法，把word也转换为二元组的向量
    word_list_vector = dictionary.doc2bow(word_list)

    tfidf = models.TfidfModel(corpus)
    index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=len(dictionary.keys()))
    sim = index[tfidf[word_list_vector]]
    sorted_sim = sorted(enumerate(sim), key=lambda item: -item[1])

    ret = list()
    for sim_row in sorted_sim:
        possibilities_index = sim_row[0]
        # sim_row[1] is typeof numpy.float32,json cannot serialize
        possibilities_ratio = float(sim_row[1])
        if len(ret) < n and possibilities_ratio >= cutoff:
            if ret_index_and_ratio is False:
                ret.append(possibilities[possibilities_index])
            else:
                ret.append((possibilities_index, possibilities_ratio, possibilities[possibilities_index]))
    return ret


def exit_if_exist():
    """
    prevent the same cron task executing repeatedly
    :return:None
    """
    import os
    import sys
    import psutil

    current_execute_script_name = os.path.split(sys.argv[0])[-1]
    current_pid = os.getpid()
    python_cmdline_list = []
    current_cmdline = ''
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        if 'python' in proc.name():
            cmdline = ''.join(proc.cmdline())
            python_cmdline_list.append(cmdline)
        if current_pid == proc.pid:
            current_cmdline = ''.join(proc.cmdline())
    if python_cmdline_list.count(current_cmdline) > 1:
        print("%s already exists!" % current_execute_script_name)
        sys.exit(0)
