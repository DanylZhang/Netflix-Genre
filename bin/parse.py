#!/usr/bin/env python
# coding:utf-8
from gevent import monkey

monkey.patch_all()

from gevent.pool import Pool
from lib.PyMysqlPool import PyMysqlPool
from config import *
import pymysql
import time
from bs4 import BeautifulSoup
import re

thread_number = 40
pool = Pool(size=thread_number)

netflix_pool = PyMysqlPool(netflix_config, initial_size=1, max_size=thread_number * 2)


def parse_en(genre_id):
    genre = 'not found'
    try:
        # get html from db
        html = netflix_pool.query_scalar(
            "select html_en from netflix.genre_html where genre_id={genre_id}".format(genre_id=genre_id))
        # bs4 parser
        html_parser = BeautifulSoup(html, 'lxml')

        # parse_genre
        genre = html_parser.select_one('span.genreTitle').text
        sql = "insert ignore into netflix.genre (genre_id,genre_en) values ({genre_id},'{genre_en}') on duplicate key update genre_en=values(genre_en)".format(
            genre_id=genre_id,
            genre_en=pymysql.escape_string(genre)
        )
        netflix_pool.execute(sql)

        # parse_movies
        movies_tags = html_parser.select('div.title-card-container div.title-card div.ptrack-content')
        if movies_tags:
            for movies_tag in movies_tags:
                # <a> movie: id name
                a = movies_tag.select_one('a.slider-refocus')
                # movie_id start
                m = re.search(r'/watch/(\d+)', a.attrs['href'])
                movie_id = m.group(1)
                # movie_id end
                movie_name = a.attrs['aria-label']

                # <img> movie: img
                img = movies_tag.select_one('img.boxart-image').attrs['src']
                sql = "insert into netflix.movie (movie_id, movie_en, img_en) values ({movie_id},'{movie_name}','{img}') on duplicate key update movie_en=values(movie_en),img_en=values(img_en)".format(
                    movie_id=movie_id, movie_name=pymysql.escape_string(movie_name), img=img)
                print(sql)
                netflix_pool.execute(sql)
                sql = "insert into netflix.genre_movie (genre_id, movie_id) values ({genre_id},{movie_id}) on duplicate key update genre_id=values(genre_id),movie_id=values(movie_id)".format(
                    genre_id=genre_id, movie_id=movie_id)
                netflix_pool.execute(sql)
    except Exception as e:
        print(e)

    return genre_id, genre


def parse_cn(genre_id):
    genre = '没有genre'
    try:
        # get html from db
        html = netflix_pool.query_scalar(
            "select html_cn from netflix.genre_html where genre_id={genre_id}".format(genre_id=genre_id))
        # bs4 parser
        html_parser = BeautifulSoup(html, 'lxml')

        # parse_genre
        genre = html_parser.select_one('span.genreTitle').text
        sql = "insert ignore into netflix.genre (genre_id,genre_cn) values ({genre_id},'{genre_cn}') on duplicate key update genre_cn=values(genre_cn)".format(
            genre_id=genre_id,
            genre_cn=pymysql.escape_string(genre)
        )
        netflix_pool.execute(sql)

        # parse_movies
        movies_tags = html_parser.select('div.title-card-container div.title-card div.ptrack-content')
        if movies_tags:
            for movies_tag in movies_tags:
                # <a> movie: id name
                a = movies_tag.select_one('a.slider-refocus')
                # movie_id start
                m = re.search(r'/watch/(\d+)', a.attrs['href'])
                movie_id = m.group(1)
                # movie_id end
                movie_name = a.attrs['aria-label']

                # <img> movie: img
                img = movies_tag.select_one('img.boxart-image').attrs['src']
                sql = "insert into netflix.movie (movie_id, movie_cn, img_cn) values ({movie_id},'{movie_name}','{img}') on duplicate key update movie_cn=values(movie_cn),img_cn=values(img_cn)".format(
                    movie_id=movie_id, movie_name=pymysql.escape_string(movie_name), img=img)
                print(sql)
                netflix_pool.execute(sql)
                sql = "insert into netflix.genre_movie (genre_id, movie_id) values ({genre_id},{movie_id}) on duplicate key update genre_id=values(genre_id),movie_id=values(movie_id)".format(
                    genre_id=genre_id, movie_id=movie_id)
                netflix_pool.execute(sql)
    except Exception as e:
        print(e)

    return genre_id, genre


def parse_main():
    async_result_container = []

    # 用于显示耗时
    start_second = int(time.time())

    genre_ids = netflix_pool.query_column(
        "select genre_id from netflix.genre_html where status_en in ('only_genre','genre_movie') or status_cn in ('only_genre','genre_movie')")
    for genre_id in genre_ids:
        # parse_en
        async_result_en = pool.apply_async(parse_en, [genre_id])
        async_result_container.append(async_result_en)
        # parse_cn
        async_result_cn = pool.apply_async(parse_cn, [genre_id])
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


if __name__ == '__main__':
    parse_main()
