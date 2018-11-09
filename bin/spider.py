#!/usr/bin/env python
# coding:utf-8
from gevent import monkey

monkey.patch_all()

from gevent.pool import Pool
from lib.PyMysqlPool import PyMysqlPool
from lib.Utility import *
from config import *
import requests
import pymysql
import time

thread_number = 20
pool = Pool(size=thread_number)

netflix_pool = PyMysqlPool(netflix_config, initial_size=1, max_size=thread_number * 2)


def genre_spider_en(index):
    url = 'https://www.netflix.com/browse/genre/{index}?so=az'.format(index=index)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': 'memclid=727bbf3e-854c-4f6a-b595-5369f2b45dac; flwssn=25bf0e0b-ca55-4189-a431-b6667b17c4c8; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABQUM4SMW55i5Uqs_tD6Wr40YNNcirE-sDU.%26dt%3D1541567777920; nfvdid=BQFmAAEBEDn2MGRJSbo3uUNAx0tMgYdgY746Kng8ca3eY%2FTsgvSqJDtdBOmLMW6TJSfgkC1OeBB7tFFdjJRwgAYJHIsW3BQOkxo24Z7m3oNtxkcsPHiElB0%2FNVt%2B9rT%2B5C0X8ZEjdAQwHi3lKykgaMak1XV2NCTy; NetflixId=v%3D2%26ct%3DBQAOAAEBEFspPbWZ1b9GbUsR3CFDm7mBsJaOoDcXC9D-fm-dVcgRlw-MNzvXdiWMIfbNwm3_x43fFOHLqvLgEzDSPUUCewFG-UdJKb3lDW9XACK9S9U3kj9CeZhn3AMcQTzAZUgEQsIq-39bBbKiGIckTvWJu99Wc0ZYm33NXN4c0phad3paIQ9JHyv347zRs_yU3TT7amY_S2ub2Zj_QcQRyJzbLTtFX-lHOyNX7jwVi9jzHyEQOl0NlqD96iUstxjAsr29qyPrZJiktiuM2Bg5c_Y_z6Op0YXtryTe21C8SjS24w-DxjvyZMHw_KbeOn-Opf6Cx5ijud2mJ8Y1zahTwcRAQjqR250oRQr8QX3KFPJighiO0AOMjwgeqHaZHdmLLttT5ypy4MwllNJm8NAHXsWeCQxusHsTSjczHwYh_00Sumfd2frJoqRxjzopoEJWVkqYIaup8GVfxcJnVxnRqq3ipvVcFD39m0LoEJSc0pykOx2QQZvs1qVfBHWEyzF-0k8vzp2DS0axegLki2CSgmO90mbeuNDoMMJdPgiEgE0YTM9PVCLdZ7qPLBhJRZxSQUlv0ZCcv6CIwvIagXez668m3jQL4g..%26bt%3Ddbl%26ch%3DAQEAEAABABSsGmJ3CwNB2X4q8EJPzDrEFAXM0MC6dbY.%26mac%3DAQEAEAABABTa4IG8igOLkl8Vfvw_emiWrwRhBKBxq3E.; clSharedContext=97b2be6e-c2de-491b-8440-1a31c679ba53; profilesNewSession=0; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE=HK%3AEN-US%3A1e568c77-58f4-4e79-a636-fce80fbc30ee_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-T=1541567786095; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-NB=HK%3AEN-US%3A89cf8219-ef25-4488-bb94-a59c5eef3634_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-NB-T=1541567797538; cL=1541567861397%7C154156785256328690%7C154156785262316249%7C%7C6%7CX3LKIHMH5VCO7I7B7N2PYFB6ZE; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A28572%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A28696%7D%7D',
        'Host': 'www.netflix.com',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        html = response.text
        if html.find('No matching titles found') > 0:
            status = 'only_genre'
        elif html.find('Unable to load titles at this time') > 0:
            status = 'no_genre'
        elif html.find('title-card-0-0') > 0:
            status = 'genre_movie'
        else:
            print(html)
            status = 'error'

        sql = "insert ignore into netflix.genre_html (genre_id, html_en, status_en) values ({genre_id},'{html_en}','{status_en}') on duplicate key update html_en=values(html_en),status_en=values(status_en)".format(
            genre_id=index, html_en=pymysql.escape_string(html), status_en=status)
        netflix_pool.execute(sql)
    except Exception as e:
        status = 'exception'
        print(e)

    return url, status


def genre_spider_cn(index):
    url = 'https://www.netflix.com/browse/genre/{index}?so=az'.format(index=index)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': 'memclid=727bbf3e-854c-4f6a-b595-5369f2b45dac; flwssn=25bf0e0b-ca55-4189-a431-b6667b17c4c8; clSharedContext=97b2be6e-c2de-491b-8440-1a31c679ba53; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE=HK%3AEN-US%3A1e568c77-58f4-4e79-a636-fce80fbc30ee_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-T=1541567786095; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-NB=HK%3AEN-US%3A89cf8219-ef25-4488-bb94-a59c5eef3634_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-NB-T=1541567797538; nfvdid=BQFmAAEBEDCN6qUgU8OhD6OUCR8w1gRgHV19lhqXGXq9Y12MWR9TYCPCxg%2BZJCQCbH2vpdX8OQKamTkbcxVMbripyB9XmOMTQRU2zSQ6WDTNbILMuhCCSaHSzIrDzP87i5RyCRxMuiSY2rLknT34tq6E6ve%2BYC%2Fn; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABTYSb9Uje6T6Ji2K75DlRpHLn8KKLLxAno.%26dt%3D1541567913378; NetflixId=v%3D2%26ct%3DBQAOAAEBECwaDSYy1U0KJ3t4HyG_QxyBwPFfaV65zSFHzB6mFlCHtBgRnsuTV9Ai1gp1gQuBpjm3yQDlEIFeDSMoNpvMpeVw2XrGdxevkMtxX9Gcf533XnoBE8aAQNs4_gIVzYTz1BywAzdmkfPl56wKckUkFU6b4jhAujJo1uugpFnmMfvjodQPWTgDSphFgs6E8Tdw95NNVia1ktGy-vnImoz73LoTbGt0GIJO5dqGKd86xi3ZXFseCPFy2_OWlgBsBmwPnFFyOoSPpO_WPzmN_4OSCcJRAGP13pJrcDE_wW7p72b1PevbpLU8kOuvmy-ks8s4LhKPIoEwNWwEwcLXrbxSN_mmMTu8n8RFZsvdG4ISrP9R2ievhcLKa8XH_6m8m2eZulZAzhBSv35zde8jx5a6A7aG5Rja_UK-g90ZD6GlEUlGK2ccDIs0emqvaaQUsMUnr3XSN75HTGkUXWoC62PgeX_RNIK5m-hsprB74xHpATGkx3_x8yiwWDTXUBPJ2KLdsmz-grotjW837bcz7CmnMhELzm8twkJ5xa-TSZzlOJ0Yke_vMl82iHUtCRlxfrzQMvqmLT2JFsBFQ1vg2_HfcdG-11Hp9QkgAUJp7Op10IMI4-4.%26bt%3Ddbl%26ch%3DAQEAEAABABR2qUDjXmCKqPirWBY-A2HWvxQD_awkb4M.%26mac%3DAQEAEAABABS4QMOftQH3v9G3Hd7tk0EH-usSeHPLdmc.; profilesNewSession=0; cL=1541567943998%7C154156785256328690%7C154156785262316249%7C%7C9%7CX3LKIHMH5VCO7I7B7N2PYFB6ZE; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA=HK%3AZH-US%3Ae6ed455a-362f-437a-87ce-1c16312c02d9_ROOT; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA-T=1541567950238; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A28795%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A28345%7D%7D',
        'Host': 'www.netflix.com',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        html = response.text
        if html.find('沒有符合搜尋的影片') > 0:
            status = 'only_genre'
        elif html.find('目前無法載入影片，請稍後再試') > 0:
            status = 'no_genre'
        elif html.find('title-card-0-0') > 0:
            status = 'genre_movie'
        else:
            print(html)
            status = 'error'

        sql = "insert ignore into netflix.genre_html (genre_id, html_cn, status_cn) values ({genre_id},'{html_cn}','{status_cn}') on duplicate key update html_cn=values(html_cn),status_cn=values(status_cn)".format(
            genre_id=index, html_cn=pymysql.escape_string(html), status_cn=status)
        netflix_pool.execute(sql)
    except Exception as e:
        status = 'exception'
        print(e)

    return url, status


if __name__ == '__main__':
    async_result_container = []
    genre_ids = netflix_pool.query_column(
        "select genre_id from netflix.genre_html where status_en is null or status_en='error' or status_cn is null or status_cn='error';")

    # 用于显示耗时
    start_second = int(time.time())

    for genre_id in genre_ids:
        async_result_en = pool.apply_async(genre_spider_en, [genre_id])
        async_result_cn = pool.apply_async(genre_spider_cn, [genre_id])
        async_result_container.append(async_result_en)
        async_result_container.append(async_result_cn)
        if genre_id % 4 == 0:
            pool.join()
            for async_result in async_result_container:
                _result = async_result.get()
                print(_result)
            # 用于显示耗时
            end_second = int(time.time())
            print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), 'elapse:', end_second - start_second)
            # 本轮结束，重置下一轮的开始时间
            start_second = end_second
            async_result_container.clear()
            random_sleep(3, 12)
