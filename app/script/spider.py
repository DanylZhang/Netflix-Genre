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
        'Cookie': 'flwssn=81cdbed5-6252-4498-9d28-b24d3ff12c1e; clSharedContext=c876bf49-0180-4c20-b9eb-427c7f0e6160; memclid=f4825702-3f31-4533-a1fa-e04ad18b1b62; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE=HK%3AEN-US%3A84f956cf-0999-4c8a-9609-551de1cbc697_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-T=1542187427958; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA=HK%3AZH-NZ%3Abb9bd1c2-39d0-4bf6-af0e-74055a694d98_ROOT; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA-T=1542187499592; cL=1542187506887%7C154218745112448183%7C154218745132900689%7C%7C8%7CX3LKIHMH5VCO7I7B7N2PYFB6ZE; nfvdid=BQFmAAEBELbW6COS%2FTtEEvIYMs8EMstgRgnPrzOGQk7mgEHwUQv0whW2kjlGbTCt583uRH9qws9P9ZGa7Q0tRpTrC8vIRh6K1VtQX8vxQEC8sVZ%2BKXhTctH%2FxlynwJYqPTdtFPonMy31pWb1F8L0JUZn3ImXkRiA; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABRhQdWjymQspP68n8DJTGETUAMG1H4V15E.%26dt%3D1542187541978; NetflixId=v%3D2%26ct%3DBQAOAAEBEJp-u1yxmd_m_ALa7MTJWgGBsJQrwoajmjUgrDM2JX334-p3wrUOxD33eGOrd4-IDxgagd4HbHrLp8qvB-xNXN1Zd1wxG-wskg39a87JXAgDM362KqOVis053iomhentOZqykFtx96fs3msWX9xBYaEh1Yq1-fmS9q8js9o2jw0J1HyH2Lw2yzpNvkm8n__Y2Z37aA5w3KxXEnN5S79Ckz57P3vK0fmyUSMBjcvXCjrs09IH2GttsgbbvEMnXxPhNz5By_TSNIXUFvK8Jmv5vUyVOu9WcuAhg9lReIrZp43SYIvIkt3rKyIA8XiwMdCvbrUmq-oXVWuPhsTivQkqe9CGeI0rokxnYQ1nb34-rcM1htOSf1h0zYg3jWEbMMls4aSKoG28WFt4ChNpRqwecsENuwbQaLBfUsruAKB6v_4kWcdu3FWZ8MYLQKnZUmW1kRE6IRc3iXe6lcMwUHVS6Dx_qr6EqoNFxkpQuz9NjPi9Er2FmLoh0SGniuIpNlinknS5McSpTf97bbf04lELJ8W_8h6K7IeeVfdklbiO_BXUiiLVJEfBfiQEbCyPywWgm_zLxmEPhagEPK4CIy5w6BXuLg..%26bt%3Ddbl%26ch%3DAQEAEAABABSsGmJ3CwNB2X4q8EJPzDrEFAXM0MC6dbY.%26mac%3DAQEAEAABABTWUfg6eGDYiQIZH_UTX22pvcOotL18304.; profilesNewSession=0; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A10101%2C%22throughputNiqr%22%3A4.255755625273864%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A5678%2C%22throughputNiqr%22%3A4.16172730613745%7D%7D',
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
        'Cookie': 'flwssn=81cdbed5-6252-4498-9d28-b24d3ff12c1e; clSharedContext=c876bf49-0180-4c20-b9eb-427c7f0e6160; memclid=f4825702-3f31-4533-a1fa-e04ad18b1b62; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE=HK%3AEN-US%3A84f956cf-0999-4c8a-9609-551de1cbc697_ROOT; lhpuuidh-browse-X3LKIHMH5VCO7I7B7N2PYFB6ZE-T=1542187427958; nfvdid=BQFmAAEBEBF39UQRsBWEadjSsOVeV31g%2FfnnvzRhJKirvsXcJ03GsuATu5sah78ZFZ%2BBLERPtR1tb45OevW%2F1%2BWeZor6sGaliCRsftDulEiPHFRu9pg1AzdiRuDh1RUnNfI8USr01ZbH5A7yKnSp2MULBiEJSOkm; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABQMsZIX7M-SY_9ik9KY2u3CODEi7Oea0wY.%26dt%3D1542187496945; NetflixId=v%3D2%26ct%3DBQAOAAEBEJH-UhuggDy5apKsjx8HqL2BwKmVjQClu2w3gGHQYmI_TIR07PVd7emh4V4bdA5WNVHD20BGcST-0MHpuROwcTohVQybCN401BBWcUTPMx8WzWXK5E-8c80ZeABm_AAdzIavEgwebpi4RmOjbko-4cjLIUfVWdU8kH8N4Y_Efvb529KpU6zKYZR6gr0hC-lWodcTfLNdGbBzpNtVuitpy6emo7VzgsrLNjMFhDd9q2gGUzRIEVsplk-HklrKqcxYCxheHws7g3IvYP3P-dIQyZeAfBwXB0hhr0YeRAefRd1DGs2OD5E_q7tEXmLX7d1Rfshf4n8tCuQd-8hUVvgTr4jn5UPX4jlEf9uT-4mFUtq6TiGcMW0se5yKJ8AuxM4HkWgDmw-R-9m1Dhlt-dV_jXSnRV_9QEbrs8wP0RvqcSf7AWmGBt5dzRfSrJlzHsFrfhE5k5HP6ZUwho_wwch7PfFE4d0sRlMn6NWU1wEHO_RA5z8_Xdqtsc0-KDO-N7vHnvJCcOd7WutQ6eYSYw4oF1rKGg2Y6OkYiDIuJXUQfwnTI8CwMdXbIVebMI31iUtPMi9QLXWgtZUA_4x8djdP_twAn-ZWP_06xwGZy7xKxXWmTNo.%26bt%3Ddbl%26ch%3DAQEAEAABABR2qUDjXmCKqPirWBY-A2HWvxQD_awkb4M.%26mac%3DAQEAEAABABTgbCKOFnLtz5IVvC0T_3R3rd0DCidNPH4.; profilesNewSession=0; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA=HK%3AZH-NZ%3Abb9bd1c2-39d0-4bf6-af0e-74055a694d98_ROOT; lhpuuidh-browse-6DO4O2SYXZBSHE5BV7COFBZAEA-T=1542187499592; cL=1542187506887%7C154218745112448183%7C154218745132900689%7C%7C8%7CX3LKIHMH5VCO7I7B7N2PYFB6ZE; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A8444%2C%22throughputNiqr%22%3A4.255755625273864%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A10101%2C%22throughputNiqr%22%3A4.255755625273864%7D%7D',
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
