import json

import jieba.analyse
import pandas
import pymysql
from flask import Blueprint, request

# blueprint
from app.libs.PyMysqlPool import PyMysqlPool
from app.libs.redprint import Redprint
from app.script.config import netflix_config

genre = Blueprint('genre', __name__)
api = Redprint('genre')

netflix_pool = PyMysqlPool(netflix_config, initial_size=1, max_size=3)


@api.route('/search', methods=['GET', 'POST'])
def search_genre():
    genres = descriptions = decades = search = []
    movie_id = 0
    if request.json:
        try:
            genres = request.json['genres']
            descriptions = request.json['descriptions']
            decades = request.json['decades']
            search = request.json['search']
        except Exception as e:
            try:
                movie_id = request.json['movie_id']
            except Exception as e:
                pass

    if movie_id > 0:
        genre = netflix_pool.query_all_dict(
            "select * from netflix.genre_movie gm left join netflix.genre g on gm.genre_id=g.genre_id where movie_id={movie_id}".format(
                movie_id=pymysql.escape_string(str(movie_id))))
    elif (len(genres) + len(descriptions) + len(decades) + len(search)) == 0:
        genre = netflix_pool.query_all_dict("select * from netflix.genre limit 1000;")
    else:
        where_str = ' 1=1 '
        if len(genres) > 0:
            genres_str = map(lambda item: " genre_en like '%{item_en}%' or genre_cn like '%{item_cn}%' ".format(
                item_en=pymysql.escape_string(item), item_cn=pymysql.escape_string(item)), genres)
            where_str += ' and (' + ' or '.join(genres_str) + ')'
        if len(descriptions) > 0:
            descriptions_str = map(lambda item: " genre_en like '%{item_en}%' or genre_cn like '%{item_cn}%' ".format(
                item_en=pymysql.escape_string(item), item_cn=pymysql.escape_string(item)), descriptions)
            where_str += ' and (' + ' or '.join(descriptions_str) + ')'
        if len(decades) > 0:
            decades_str = map(lambda item: " genre_en like '%{item_en}%' or genre_cn like '%{item_cn}%' ".format(
                item_en=pymysql.escape_string(item), item_cn=pymysql.escape_string(item)), decades)
            where_str += ' and (' + ' or '.join(decades_str) + ')'
        if len(search) > 0:
            search_str = map(lambda item: " genre_en like '%{item_en}%' or genre_cn like '%{item_cn}%' ".format(
                item_en=pymysql.escape_string(item), item_cn=pymysql.escape_string(item)), search)
            where_str += ' and (' + ' or '.join(search_str) + ')'
        sql = "select * from netflix.genre where {where_str}".format(where_str=where_str)
        genre = netflix_pool.query_all_dict(sql)

    genre_df = pandas.DataFrame(data=genre)
    genre_ids = genre_df['genre_id'].values.tolist()
    if len(genre_ids) > 0:
        genre_ids_str = ','.join([str(genre_id) for genre_id in genre_ids])
        genre_movies_count = netflix_pool.query_all_dict(
            "select genre_id,count(*) as genre_movies from netflix.genre_movie where genre_id in ({genre_ids_str}) group by genre_id".format(
                genre_ids_str=genre_ids_str))
        genre_movies_count_df = pandas.DataFrame(data=genre_movies_count)
        result = pandas.merge(genre_df, genre_movies_count_df, how='left', on=['genre_id'])
        return result.to_json(orient='records')

    return json.dumps(genre)


@api.route('/wordcount', methods=['GET'])
def genre_wordcount():
    genre_list = netflix_pool.query_column(
        "select concat(genre_en,', ',genre_cn) from netflix.genre where genre_en is not null limit 5000;")
    genre_list = filter(None, genre_list)
    genre_list_str = ', '.join(genre_list)
    tags = jieba.analyse.extract_tags(genre_list_str, topK=100, withWeight=True)
    return json.dumps(tags)
