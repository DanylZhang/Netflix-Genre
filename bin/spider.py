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

thread_number = 10
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
        'Cookie': 'memclid=204dcc74-9e00-4c46-9609-244483b5e632; _ga=GA1.2.970173586.1541291884; VisitorId=002~204dcc74-9e00-4c46-9609-244483b5e632~1541291884618~true~1541291884628~; clSharedContext=97bfab6a-33ac-4bdb-b162-419201246907; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4=HK%3AEN-US%3A5259aa93-17d1-405f-9e09-8c986fbfa0c8_ROOT; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4-T=1541423793496; flwssn=37b83473-e213-43bc-9db1-fd3e97eab964; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA=HK%3AZH-US%3Adc357eaa-0486-48fd-9578-94b8a77f066a_ROOT; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-T=1541424308191; cL=1541424308187%7C154142428272457868%7C154142428286233603%7C%7C5%7CMNO5MF6VDZDIFGTRHOE6NDVG6A; nfvdid=BQFmAAEBEErk4pR15P3QhVUKla%2BhCxNg5eRXgiKxLm%2F5SaC2OlsLWiBb7po2X%2F9PyhE3voOLBSWhggdZivy9AFBCsBuFj2Q3%2FjWDZdtz8qD1AaR4KLQdq6Oc9M7Ikyj5XVM6Vz0aCEJiiQBfM7%2Fvf3Un26K1NdMT; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABRTEQX-p2x3W9QEvJYX7ZfVEccuGqkuUN4.%26dt%3D1541424314725; NetflixId=v%3D2%26ct%3DBQAOAAEBED4sHuJ4-kfx0Uxm_VrNePOB4EhiN6zdvtivBmvoVaPhgD9pV5r64AJBJHLeuzs563VSdWcTqgYCIeUVMlUakQfxtYDsctj3P8WwHjriIATqDxSdCksv8drdesmhDSu_ZdpB9qcsOp5XzNqSa5P0azqy89eJd-54UPqNDUpPoImTyLYvQU98cM7EmSGdPriiMe99iXl0XXby3XADREww7pQ_OjTSAJEX5dnW0KaGleCsN1LRClpz9TRTID7qEiZNGq27tBgDFmDiyVc9dDfxLn62qib1XVQ4Pr3NArpGg5XquyqAaX_Ismuz3RFJb-TvUdGwWaGPGmr4jNz17ezZJzXpZOo3SgdOXF2gY1LEYMSx24sY8LDfmax0ksDvp5A0Dm7H6ADjtqWgSgRxrFHQ3Nkeq6LymgrhDvfiDL51dQAVKy5yEXpg3-ZShm0AJbZkH8WtH1et920drOO_5rxjhItxwL_Ujs4ngYHJOZzN6l19vQkFiNRh9aUKpy8IfPTOFVR1mS386Mm3Xauu9fjM61Bx1-MZqg4LHBrBxT4diYtef1JOl0ZB34uReLwrLG57tX-A9IqqzX70IXEsCMAxcdyBbaJzuP7ETEp5zir_QQMmZH_kF0sfmD86-r1T-T7bTEwG9ZvpH7Vn3UL02nc9ugQ5OA..%26bt%3Ddbl%26ch%3DAQEAEAABABSlbku3ccdUhGpCiLMog9FC_H70jPcyJoE.%26mac%3DAQEAEAABABRQrRtscc5L-Ys2GehXcYHdpn_SmxnkEjU.; profilesNewSession=0; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A18300%2C%22throughputNiqr%22%3A0.1090487599449921%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A18273%2C%22throughputNiqr%22%3A0.1090487599449921%7D%7D',
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
        'Cookie': 'memclid=204dcc74-9e00-4c46-9609-244483b5e632; _ga=GA1.2.970173586.1541291884; VisitorId=002~204dcc74-9e00-4c46-9609-244483b5e632~1541291884618~true~1541291884628~; clSharedContext=97bfab6a-33ac-4bdb-b162-419201246907; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4=HK%3AEN-US%3A5259aa93-17d1-405f-9e09-8c986fbfa0c8_ROOT; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4-T=1541423793496; flwssn=37b83473-e213-43bc-9db1-fd3e97eab964; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA=HK%3AZH-US%3Adc357eaa-0486-48fd-9578-94b8a77f066a_ROOT; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-T=1541424308191; cL=1541424308187%7C154142428272457868%7C154142428286233603%7C%7C5%7CMNO5MF6VDZDIFGTRHOE6NDVG6A; nfvdid=BQFmAAEBEFTsAczQSplE9jG%2F39uyQ2RgoSyjM7km%2Bqs9BPWcCksJXnP%2BKQD74d3wsFZ150SZ%2FO7EbVCRWBWh%2FfTPdgTZxHyh07V95wypFKCPANRk8Gj4zESEOsJ6zSXENCDTUMCAaRshQry7V0nXco3M3UEWWze1; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABQRhrVq8lEQ7UPKiWWhmbpypyTGxsusURE.%26dt%3D1541424356924; NetflixId=v%3D2%26ct%3DBQAOAAEBEPX7YNhoNxjf0LLWv6kq6PGB4CaG0JlXZauxfd0nezCzn1KcNZzSZsQ3GFSYDPiSf4o4bbN4QSIRrckChZGnqHpUgZpslR-AoYO7TLuvmmd5fZBlaBrxaW9gW6iL1BLeCMJuR8N9aimXkBzTtCTAVALBJ-gsTNjhruC0XtrMzJGdxT_VIdcMfSrEc4nvRTjOUhyAmrQeprG2jt9TyiWcaUg30vAsb4PIgisP1xfh8BfpsY7vpwMStUSZn82ugNjPuL6wYU33wkGPmaIOP7xiUwf6b1y8wHPzRWTfJw8GvJd53Sbuiz0CQtbx5YOvv1nm3wY-ju3p31F5YEstDerlnoMusXQ1yWtI6rA0hX6SWKsTXJegQZsZhqatm6DgHLd6ke9u7vMHKIAzrM6L9dqgsakqwUbxStG3c_-1vPkg6kwe8h395D3V4XeGEYcv4zky6LDrEc762NAjjAyRix8o37jDGRJNHJ4k0-uGxTxUAR3nC60mEW3zOt8FwdFUWMIiY7EBK17gyZPb5Bh7zR4iSLXF_-_K0tLIaBVX06l9jQvT-CnaDzDPWHeM9PA_NLdb0UzONI5pr7216QlqlVJgDodcfiIg4uHqVuir0VuHfl3ynok3PpZ7i-5yvwIzeVO1ApgJzMP86mL410SrOjF9jbeXSQ..%26bt%3Ddbl%26ch%3DAQEAEAABABTvm4XzFELfkvfOoZdVYfTDMD-6XcUAcMg.%26mac%3DAQEAEAABABT66YkRKp718qgCgq9GvvIuEJw2eJwI5OU.; profilesNewSession=0; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A18273%2C%22throughputNiqr%22%3A0.1090487599449921%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A18273%2C%22throughputNiqr%22%3A0.1090487599449921%7D%7D',
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
        if genre_id % 3 == 0:
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
            random_sleep(3, 10)
