#!/usr/bin/env python
# coding:utf-8
from gevent import monkey

monkey.patch_all()

from gevent.pool import Pool
from lib.PyMysqlPool import PyMysqlPool
from config import *
import requests
import pymysql
import time

thread_number = 3
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
        'Cookie': 'memclid=204dcc74-9e00-4c46-9609-244483b5e632; _ga=GA1.2.970173586.1541291884; _gid=GA1.2.668381852.1541291884; VisitorId=002~204dcc74-9e00-4c46-9609-244483b5e632~1541291884618~true~1541291884628~; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA=HK%3AZH-US%3Aba95a9f7-9fd9-48fb-9779-24bcb3a548bf_ROOT; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-T=1541292781390; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-NB=HK%3AZH-US%3A106459c2-fafc-4673-9bc4-5b4f425b650d_ROOT; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-NB-T=1541309508626; lhpuuidh-browse-C77U3YEDABB5FDDMFZR4M53YFQ=HK%3AEN-US%3A1dc88bc6-ab0b-4b61-a673-1c51290fa476_ROOT; lhpuuidh-browse-C77U3YEDABB5FDDMFZR4M53YFQ-T=1541325584554; lhpuuidh-browse-D3F6F6ACEFAGFNAGHQRD7GK24Y=HK%3AZH-US%3Ac9dc9dd4-be2c-40fe-aff2-abfd4f51948b_ROOT; lhpuuidh-browse-D3F6F6ACEFAGFNAGHQRD7GK24Y-T=1541325607427; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4=HK%3AEN-US%3Ab27e0f44-c91a-4f2f-a5fe-961a62c367d8_ROOT; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4-T=1541325745495; nfvdid=BQFmAAEBEHMKV9d4FBWOs2dPLV0Ejhpgz9jL8j4DcfmI4LBLcrCUmi4ia6FRkzv7gzz6C0bQM1ZDa9eatlvNBCikdbQo01BywjiHoFwr2jZXrX05Ucz4FrLxwKVF5HrpiQpe7BoZQ4Us3I1ZPXXN2HE00Tfi8IRk; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABRN4muDYhRVWlNgZYKKyg2sDhcVLxbO0Ug.%26dt%3D1541331200975; NetflixId=v%3D2%26ct%3DBQAOAAEBEC91WqfXI5eTULgjRnTSbRGB4DopE6vKUfmt1XslSK5lv20U7gTXT_CNxDmlTCbJrzgWvV4k-9VfyrCvyDAXifaT9XZxxc1IEGN5PxRRw4JwAqJ2BGBRrX39o-tHJPfROznbWjnhEhlzFLqRGKEDNXQmV3iVwBzGO1ju7ovGXIbY5iAvvyHh25izqkAIaYMymeI-QEzahcxXIp3NEtXJodsxaiESuKyB-Qeec1xF0B_qc5mskjwNPluJfGrDZV7nsHFmAoHbJtrnlkMqvcrJ3ANtObFb5vwnbU90GS0c7R9axr2ZuRyyhimrULNsAiTkkeasxvYFV2nPP5lRcpzFVw1ohGO6pYwrLU60rQAflIdFO4FadV6EgiYaK07_wORcHlWN_BsogEIQXuIwSvw7h5ygZUxS9ol975ypdIlkIAZmuO7hpVT7sKOvYkhF0FBvzczav8OQhVS03UUTg5tTMbhcD342WiBBLdh6iiWw8wiOvuiwV1ctR05ZBE73dN0fyVe8Zgj0bVXdp1AYpAuCLptyOSfGi_VkLuXkpJSukjaEfpqC6b81MSVHHztXDpqpzhJi9nB-KJdZzqfO6i08CjTxYWva937U-cjKAHrmScwCSM57sis4gUjHjZ1wpYT9_2GPj6P00veFVkfgNY9moit_wA..%26bt%3Ddbl%26ch%3DAQEAEAABABSlbku3ccdUhGpCiLMog9FC_H70jPcyJoE.%26mac%3DAQEAEAABABQaTmf6gleQumbxPZV8l1byZAddhgP2nP4.; profilesNewSession=0; cL=1541331218232%7C154132492990387184%7C154132492998646422%7C%7C32%7CWNE4DQR27JAVRKFSTM3GIXPBMA; clSharedContext=14ebc140-9f5c-458e-9753-22eb7c5dd4e6; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A18140%2C%22throughputNiqr%22%3A0.1090487599449921%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A18140%2C%22throughputNiqr%22%3A0.1090487599449921%7D%7D',
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
        'Cookie': 'memclid=204dcc74-9e00-4c46-9609-244483b5e632; _ga=GA1.2.970173586.1541291884; _gid=GA1.2.668381852.1541291884; VisitorId=002~204dcc74-9e00-4c46-9609-244483b5e632~1541291884618~true~1541291884628~; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA=HK%3AZH-US%3Aba95a9f7-9fd9-48fb-9779-24bcb3a548bf_ROOT; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-T=1541292781390; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-NB=HK%3AZH-US%3A106459c2-fafc-4673-9bc4-5b4f425b650d_ROOT; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-NB-T=1541309508626; lhpuuidh-browse-C77U3YEDABB5FDDMFZR4M53YFQ=HK%3AEN-US%3A1dc88bc6-ab0b-4b61-a673-1c51290fa476_ROOT; lhpuuidh-browse-C77U3YEDABB5FDDMFZR4M53YFQ-T=1541325584554; lhpuuidh-browse-D3F6F6ACEFAGFNAGHQRD7GK24Y=HK%3AZH-US%3Ac9dc9dd4-be2c-40fe-aff2-abfd4f51948b_ROOT; lhpuuidh-browse-D3F6F6ACEFAGFNAGHQRD7GK24Y-T=1541325607427; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4=HK%3AEN-US%3Ab27e0f44-c91a-4f2f-a5fe-961a62c367d8_ROOT; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4-T=1541325745495; cL=1541331218232%7C154132492990387184%7C154132492998646422%7C%7C32%7CWNE4DQR27JAVRKFSTM3GIXPBMA; clSharedContext=14ebc140-9f5c-458e-9753-22eb7c5dd4e6; nfvdid=BQFmAAEBEFkhPQitfIZcJq%2BuJ4MmTTxgfhI446hl2y4pW83JWEGN6%2FQzPLGFlr7zZ46VLWA58et%2B0NwWSMbyjeoGkuPBFFhSrSWLXdfzqzFUQyw23UXR3bm3QIvmUxGzAsYNIjCSYD3%2BAG5gLYaUgqZd6EcoVmmy; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABSCwMYM0TeVW6yjfQo_pte9WUgi7GD6m5s.%26dt%3D1541332200009; NetflixId=v%3D2%26ct%3DBQAOAAEBEAeS5Z7jG5MVVSp_B6GluWCB4LFJlJ10Eejh7pX9hsjMon6Qbcy9AtuvS26LWJ5Kmwn-CyG_a3qzxc7U-5M1ffPTBCGLz658RGhRIu64XNvOhvTJBqZkDzUg90nzEgcjPZwoENmY2oFw4TDujQLtSfbFSjyrLo8BBkjphKgjIIceEPkketHQ1IY1i45HgPYlSoZBKx5m7NgGAJPHSfUiHPRdR_au9DZxdl4eOmJg4O_Rcp4NLR_jKXO_E-_MA8q2EJsVzi3GZ2yn4pJE7vSwBhKp2HEiyD2MzD6QIGsstuEeDGi6jpOkNUgb9srUPLkE4YJ2G502fRNi_f16LqyYqHNssdGO7DEpVHbXEHoDgbPxoGLlZSGxqba9XUdl70aDOafRMxwm7L4eRy4fI9snWvQMS_80_RypwVmDDMxIy7GzLbwffLd6SJcywx9PH0pgD9NZFGRNbpnXTiuEE11pdMuIKeJK3m7TGs5WNNHmJVUXbVt833Qq3UoY_DoI7o5z3kcO3CIRBcfF4gg2tn8QU8KIT9B1D35S1MmApdzoP4_AEFgTrHpEsl3_Ku2Xfr2oEXkdKf9o-bT3V3JaXjkFD7PNwBCAPNl9tdFhHo0BGhNYPpiT0pCjZjDs0RKUvIQ2BvG7ebvXXdYabWgap-sTLlEOpg..%26bt%3Ddbl%26ch%3DAQEAEAABABTvm4XzFELfkvfOoZdVYfTDMD-6XcUAcMg.%26mac%3DAQEAEAABABR6T78rmNJIBkqLU_skr-74oUs26xJjSHk.; profilesNewSession=0; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A18140%2C%22throughputNiqr%22%3A0.1090487599449921%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A18262%2C%22throughputNiqr%22%3A0.1090487599449921%7D%7D',
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
        if genre_id % thread_number == 0:
            pool.join()
            for async_result in async_result_container:
                _result = async_result.get()
                print(_result)
            # 用于显示耗时
            end_second = int(time.time())
            print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), 'elapse:', end_second - start_second)
            # 本轮结束，重置下一轮的开始时间
            start_second = end_second
