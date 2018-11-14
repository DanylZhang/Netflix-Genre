#!/usr/bin/env python
# coding:utf-8
from gevent import monkey

monkey.patch_all()

from gevent.pool import Pool
from app.libs.PyMysqlPool import PyMysqlPool
from app.libs.Utility import *
from app.script.config import *
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
        'Cookie': 'memclid=727bbf3e-854c-4f6a-b595-5369f2b45dac; clSharedContext=97b2be6e-c2de-491b-8440-1a31c679ba53; profilesNewSession=0; cL=1541986651857%7C154198664474639479%7C154198664412383140%7C%7C20%7CX3LKIHMH5VCO7I7B7N2PYFB6ZE; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE=HK%3AEN-US%3A4b7fe74d-30b7-4e70-b695-750795ee4457_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-T=1541986657787; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A18841%2C%22throughputNiqr%22%3A16.347360494581988%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A18841%2C%22throughputNiqr%22%3A16.347360494581988%7D%7D; netflix-sans-normal-2-loaded=true; netflix-sans-bold-2-loaded=true; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABRaBMXqN6ARm1U_NEjp6sF6C7GC6YlItio.%26dt%3D1541986682956; nfvdid=BQFmAAEBEH8B7B5aaEg2F%2BVxlKFNQbZgHD%2BALP4S93vcVjtvww12F4MLvk3IUPSCHtjhHfXeOrsJe2b4qe4f0R%2FspyMpp9LZSt7UThPqhdqaBLlb%2FDfBljOP32aZ%2FcfL457WaevSJl3ga9KNV5iGAY%2FZ7kF4fW8B; NetflixId=ct%3DBQAOAAEBECh3O1G2G0ZisaeKEXqoY_GBsD48xP1UT2vVV3yvKOv6L45BwJIP_Ye0_lzrhp-tNYgd3rY4qpr09_k6tFA2MtWOnKMVacOPHHElN4Mz6dcSbrnx0MMa0TnokAutVp7XGwEvApmYBiCnlmN2doTMUG28KbLMleOIsj51kC4Pf-qIoGS5XF5nZrs-HJmSWFEc19xH32AYyUpW7iepRyTZpgmpVHlAWjlw96cwCXWqNuNspGbzoDM42MECQqnwJod7L02A-jBtGJYy6hqfRQy-muULaMNOW07NXzPilemKysscr5F82Mb9STJHx2SX9nZg5p50r5fOOilB0Q6fVek5L1awU9pc2F9gWBiNNcg4sLN2hXiJ0Z4EIcHPnmJC0H8dgMk9U4E0KAD2Rz5QGEe9bPmgvidE8gLSLt7YV8A-wIDnmIxhqVwB7jCYvleooIzr7Nk4-AwP93Jr3VbskCezXdlA9bxCyCIKhp4dAu16VBJgRxd-XiqLU6zm2CB17kbyZ4yO2coPLKkTLuDySuvO2W0u_3x-jTur2IMwVdS47vIkUr2FrXfEdwEF5YOpCkRi1yNB9BxARxP3NQdYHPlLyIP-kA..%26bt%3Ddbl%26ch%3DAQEAEAABABSsGmJ3CwNB2X4q8EJPzDrEFAXM0MC6dbY.%26v%3D2%26mac%3DAQEAEAABABQsJqzl9G-RB5Pwo4Hi5lskU0YdPgV-lsw.',
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
        'Cookie': 'memclid=727bbf3e-854c-4f6a-b595-5369f2b45dac; clSharedContext=97b2be6e-c2de-491b-8440-1a31c679ba53; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE=HK%3AEN-US%3A4b7fe74d-30b7-4e70-b695-750795ee4457_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-T=1541986657787; netflix-sans-normal-2-loaded=true; netflix-sans-bold-2-loaded=true; nfvdid=BQFmAAEBEBQTNXdt6KS6KBCawf74uP1gJLlhBuea1jE8leraR4b4q9sll%2FrgdAGoBLKA8pJjoTMWXCNChAjLX14txVQltnovPMuOd%2BgHL6JvuFchdrWSz0rURzbQnlFmOrJuLhhCelKx%2FjGXgWy%2B8QqAXR0nF3F2; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABQ9f2hPNYd60CUeGRCFor_NygcpbFTuRa8.%26dt%3D1541986730725; NetflixId=v%3D2%26ct%3DBQAOAAEBEAhnR8k6mvJberD2J7MsGDGBwPflj1aRz9yX-99WcgeGLjIqXl_a4RAIew9eFzsavqDL_aPL1BrLy6Wsp6ZohwVdo0pqNTSU3DJUj6eNOLQzcTdrN7adpwpn59cAAd-ArSBaDWKd3yUI1tuh6kdJC_OUXcBLuso95hdEZM_RPbCp99HGkvDgzZPiH6r0s7w--piTvTLTNqW7p4_YY2rNIJiZ7xkFGo4sqVg5GFC13Fts086rZbmxHhX1KzUsgDUuytHTU-UY2LSGbdmStmiNxDayF9OElRK3Oi_fUMcNMoseRbVmNT667d2mfraUXIX8eyTht-S0WdHnWSgCWfNnH1kgYx4-1tWVWEt9XrZVsmVMh7DLv7oKizVirVU4ricbtpNh3V1uQP03Nx9M1zue8M2BLhVjna7c0XUrRNfn1NIu0eUAyvoJskbChmIQ_yUuSpvA3oDHtn8EPbESOEkxaCPPeHrdS1cRHK6JRFqxL-TJ1NNmdQxPoj4aBxvr1cHaiX0x4gbQ-DXHjYjw_A1JoIsYTgkvFrAW8qqhPlrOlFNqZN14uxR5LdC8qZBUAUt7Fz1N0KR8JHr6bRXyySSSAiE7X49NEClR8NB-GmsJhWk23XQ.%26bt%3Ddbl%26ch%3DAQEAEAABABR2qUDjXmCKqPirWBY-A2HWvxQD_awkb4M.%26mac%3DAQEAEAABABR-6W2xlKifbGdveG-b8i1uuXazDJoTbeI.; profilesNewSession=0; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA=HK%3AZH-US%3Ad39972fc-bb3e-49ea-b8e9-1e6b325346f5_ROOT; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA-T=1541986732242; cL=1541986742950%7C154198664474639479%7C154198664412383140%7C%7C23%7CX3LKIHMH5VCO7I7B7N2PYFB6ZE; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A18996%2C%22throughputNiqr%22%3A16.347360494581988%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A18996%2C%22throughputNiqr%22%3A16.347360494581988%7D%7D',
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
