#!/usr/bin/env python
# coding:utf-8

import numpy
import pandas
from gevent.pool import Pool

from config import *
from lib.PyMysqlPool import PyMysqlPool
from lib.Utility import *

thread_number = 32
pool = Pool(size=thread_number)

apollo_pool = PyMysqlPool(netflix_config, initial_size=1, max_size=thread_number)
allsite_pool = PyMysqlPool(netflix_config, initial_size=1, max_size=thread_number)
dataway_pool = PyMysqlPool(netflix_config, initial_size=1, max_size=thread_number)


def xxx(cid, shop_type, current_cid_child_str, start_day, end_day):
    brands = allsite_pool.query_all_dict(
        "SELECT date_format(date,'%Y-%m') month,shop_type,{cid} as cid,if(rb.rbid,rb.rbid,ta.brand) id, sum(num) num_total, sum(sales)/100 sales_total FROM all_site.`category_all_kaola` `ta` LEFT JOIN all_site.`root_brand_match_kaola` `m` ON ta.brand=m.bid LEFT JOIN all_site.`root_brand_kaola` `rb` ON m.rbid=rb.rbid WHERE ((`shop_type` = '{shop_type}') AND (`cid` in ({current_cid_child_str})))AND (`ta`.`date` >= '{start_day}')AND (`ta`.`date` < '{end_day}')AND (`ta`.`brand` > '0') GROUP BY `id` ORDER BY `sales_total` DESC LIMIT 20;".format(
            cid=cid,
            shop_type=shop_type, current_cid_child_str=current_cid_child_str, start_day=start_day,
            end_day=end_day))
    brands_df = pandas.DataFrame(data=brands)
    if not brands_df.empty:
        brands_df['top'] = numpy.arange(1, len(brands_df) + 1)
        brands_df['id'] = brands_df['id'].astype(numpy.int64)
        brands_df['sales_total'] = brands_df['sales_total'].astype(numpy.float64)
        brands_df['num_total'] = brands_df['num_total'].astype(numpy.float64)
        brands_df = pandas.merge(brands_df, categories_df, how='left', on=['cid'])
        brands_df = pandas.merge(brands_df, brand_name_df, how='left', on=['id'])
    return brands_df


if __name__ == '__main__':
    months = range(201707, 201713)
    months.extend(range(201801, 201809))

    lv2cids = dataway_pool.query_column(
        "SELECT distinct cid FROM kaola.member_category WHERE mid=51631;")
    all_cids = dataway_pool.query_column(
        "SELECT distinct cid FROM kaola.item_category WHERE is_parent=1 and `lv2cid` IN (%s)" % (','.join(
            map(str, lv2cids))))
    brand_name = dataway_pool.query_all_dict("select bid as id,name as brand_name from kaola.brand")
    brand_name_df = pandas.DataFrame(data=brand_name)

    categories = dataway_pool.query_all_dict(
        "SELECT cid,lv1cid,lv1name,lv2cid,lv2name,lv3cid,lv3name,lv4cid,lv4name,lv5cid,lv5name FROM kaola.`item_category` WHERE `cid` IN (%s)" % (
            ','.join(
                map(str, all_cids))))
    categories_df = pandas.DataFrame(data=categories)
    shop_types = ['selfglobal', 'popglobal']

    writer = pandas.ExcelWriter('./{timestamp}.xlsx'.format(timestamp=int(time.time() * 1000)))
    async_result_container = list()
    result = pandas.DataFrame()
    start = int(time.time() * 1000)
    for (index, cid) in enumerate(all_cids):
        level = dataway_pool.query_scalar("select level from kaola.item_category where cid=%s" % cid)
        current_cid_child = dataway_pool.query_column(
            "select cid from kaola.item_category where lv{level}cid={cid};".format(level=level, cid=cid))
        current_cid_child_str = ','.join(map(str, current_cid_child))
        end = int(time.time() * 1000)
        print(u"cid: {}, 共: {}, 剩余: {}, 耗时: {}s".format(cid, len(all_cids), (len(all_cids) - index - 1),
                                                        (end - start) / 1000))
        for i in range(1, len(months)):

            y = str(months[i - 1])[0:4]
            m = str(months[i - 1])[4:]
            d = '01'
            start_day = y + '-' + m + '-' + d

            y = str(months[i])[0:4]
            m = str(months[i])[4:]
            d = '01'
            end_day = y + '-' + m + '-' + d

            for shop_type in shop_types:
                print(1, time.time())
                async_result = pool.apply_async(xxx, (cid, shop_type, current_cid_child_str, start_day, end_day))
                async_result_container.append(async_result)

    pool.join()
    for async_result in async_result_container:
        _result = async_result.get()
        result = result.append(_result, ignore_index=True)
    result.to_excel(excel_writer=writer, sheet_name='brand',
                    columns=['month', 'shop_type', 'lv1name', 'lv2name', 'lv3name', 'lv4name', 'lv5name', 'top',
                             'brand_name',
                             'sales_total', 'num_total'],
                    header=True, index=False, encoding='gb18030')
    writer.save()

    print(int(time.time() * 1000) - start) / 1000
    exit(0)
