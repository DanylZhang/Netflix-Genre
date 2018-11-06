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
        'Cookie': 'flwssn=78c970df-6744-49e3-a7fe-3e35fc46200b; clSharedContext=55e26e88-dd1d-4912-ae55-3540d021011c; memclid=95f39a00-5352-45a4-b467-76678b159fc8; cL=1541470712742%7C154147069516336321%7C154147069565525365%7C%7C4%7Cnull; nfvdid=BQFmAAEBEMiQaCA%2ByJtZ2KWhi%2FOBRANgWaU8gj0tj4lLCKnrVvqNwgNYzH3jYPclC%2Fqo73BcSw78xr0AEqV2b%2FBTXQ8okAEhhQ9Cg0F1U8vitRTgMFdd3geygGzeJ%2Fs8mNuD0Qivt9k%2FRFFVGP18yZdxJQd4fg3o; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABQhWdT5qqKEUOl3jOErHvYkJmfQXuCKozs.%26dt%3D1541470715002; NetflixId=v%3D2%26ct%3DBQAOAAEBENFogITs3Gh6gX9yHgKNYJ6BwAdE3TMtn1FQrSkujOVHVXQnEvYTUcVtbTXJ9d3VtzpvZM4bOfaXtfQ7mPz0lOgHvImTUr1XWGav_zebdqWhMpB3OILJygJnySDlkpNyq87MPaOYZrWFoxFAfevvktaQO5JT0HRBSrS_kBOcRvLuwYENQFc_E7blyf66-Enh_W6Q80HyeDvhjLmQrCjfuHdN6_DxuUWExM8cRwA6fWbQkBqWQZx0PTQoGHay-QATkByLo9dmbQeoQer2IlTXJXh-BZbrGGKVyIc60ZrYQzUPTbe037Ucz0afczh3lXKkhxd4P0SgIwiZr7zSWH81h2IEO8rZrXDhmfZI6mPetgdTW3zTf9Nhbfb3kNNxYyxiwYokCpoB2U4TccogIkWi9aw87Dq7Vq6f7EuOoJ7mKUGXlVFZsMCKNyQG6RBSm6K66DwJpGAWLiM4a_3FnlKiYEfpkAyB0Vipu7Ar-jNqT3wRoF0LAd7YfJ_IKgV_EmZyQ3fNtJzz-sxlPMp4l0gSCnU8WW8qI6Tb2yho8yWXKf-Uq82SNiMo7kbevp0ddYjnw8JtNdLpftFF664ktj0n35URwxvdmOTFXVnBS1c4OQcRclk.%26bt%3Ddbl%26ch%3DAQEAEAABABSlbku3ccdUhGpCiLMog9FC_H70jPcyJoE.%26mac%3DAQEAEAABABT57eUU8zQ12qxY6-1hos5tYvMNUghj4Vc.; profilesNewSession=0; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4=HK%3AEN-US%3Ab2fece81-a42c-4a51-86cc-057c223bf68a_ROOT; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4-T=1541470716807; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A19647%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A19647%7D%7D',
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
        'Cookie': 'flwssn=78c970df-6744-49e3-a7fe-3e35fc46200b; clSharedContext=55e26e88-dd1d-4912-ae55-3540d021011c; memclid=95f39a00-5352-45a4-b467-76678b159fc8; cL=1541470712742%7C154147069516336321%7C154147069565525365%7C%7C4%7Cnull; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4=HK%3AEN-US%3Ab2fece81-a42c-4a51-86cc-057c223bf68a_ROOT; lhpuuidh-browse-XLKL67CZO5FCBJHIGCOEER72R4-T=1541470716807; nfvdid=BQFmAAEBEMnU6JEVYcSQ%2BShv7V4HYURgNP8BVifMbQz8uOeCACRuTBhDWbVPu5KE9m9CUGD6gS2N5OLW6HMGnWA0pl8uNPYNi%2BL4tIy351mGcyN8aG1bfo3kTm0a59lpPW2jdyrlkV4RySv4POkQbcRmth1QkPBA; SecureNetflixId=v%3D2%26mac%3DAQEAEQABABRLZ0DZ5vaVW57-egxlsMBvMvRRZntM6t0.%26dt%3D1541470800967; NetflixId=v%3D2%26ct%3DBQAOAAEBECmdPoPdeHrhVL0cgyofM-WB0ERYy9Kg-dCGbvcyuAFzlCiMn-WO96-8Ymjc2aRPO6XNI4N9mzPcYhlEcDYGBoCnG4llxVfoEc6QazjjjhCFE6AQOLhJf0d_RnosIWGqM_SLVF20Ec7Jm9sq49uOEuM1XKMXW2iRAR1C5uXz_QsQLqjzUXfryocnVPSWrCBA0oBGyMaaEpC2T4bLfIqyqIE9eVA7CdMszGk9-bH3HeontXxuGvbPk_h2-llXPHbLjP4fbj5TjEwCdVpcR2LcMFtCKFc9eRy2lj1XnvuOHhVm6dXpX6SzRCCKjNCq6RaaQHAneficENWpwY6ooFBDakFIrjp3TRN4OvkRLqkKFbHLu1_qof6Iqm2RJkvApkFOKKxWh4CipIAecGR8n3ptXg9Ed04JIXJRs23TPD63Nt7lHsR9WS4pU6r-NxhZDX0EC7cd36oXJrMbFDdJdCW0hc-WuXMuhXZpfBour5fVSQAj3-tvnG96sQNKcuB8HAOPUDb75LVde2wMVhdL3LS1dOwQUxvIUTRqptf1dnhFTOiUBeQnKjnmEefUtXtOqXtXGQ7Hf5P-ayDztXiS71gECIQcVj617WlghqT0uCo7mx48MR9QKIB4gQM26olLVgLuudlL%26bt%3Ddbl%26ch%3DAQEAEAABABTvm4XzFELfkvfOoZdVYfTDMD-6XcUAcMg.%26mac%3DAQEAEAABABS-y3zmEjHY7889lGAwemnAzXTDgODWmNI.; profilesNewSession=0; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA=HK%3AZH-US%3A9699b5bd-9843-48bb-864c-2e2709ce93ca_ROOT; lhpuuidh-browse-WNE4DQR27JAVRKFSTM3GIXPBMA-T=1541470803083; playerPerfMetrics=%7B%22uiValue%22%3A%7B%22throughput%22%3A19647%7D%2C%22mostRecentValue%22%3A%7B%22throughput%22%3A22605%7D%7D',
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
        if genre_id % 5 == 0:
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
