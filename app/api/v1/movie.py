import json

import jieba.analyse
import pandas
import pymysql
from flask import Blueprint, request

# blueprint
from app.libs.PyMysqlPool import PyMysqlPool
from app.libs.redprint import Redprint
from app.script.config import netflix_config

movie = Blueprint('movie', __name__)
api = Redprint('movie')

netflix_pool = PyMysqlPool(netflix_config, initial_size=1, max_size=3)


@api.route('/search', methods=['POST'])
def search_movie():
    if request.json is None:
        movie = netflix_pool.query_all_dict("select * from netflix.movie limit 1000;")
    elif request.json['genre_id'] is not None:
        genre_id = request.json['genre_id']
        sql = "select * from netflix.movie m left join netflix.genre_movie gm on m.movie_id=gm.movie_id where genre_id={genre_id}".format(
            genre_id=pymysql.escape_string(str(genre_id)))
        movie = netflix_pool.query_all_dict(sql)
    elif request.json['search'] is not None:
        search = request.json['search']
        sql = "select * from netflix.movie where movie_en like '%{movie_en}%' or movie_cn like '%{movie_cn}%'".format(
            movie_en=pymysql.escape_string(search), movie_cn=pymysql.escape_string(search))
        movie = netflix_pool.query_all_dict(sql)

    movie_df = pandas.DataFrame(data=movie)
    movie_ids = movie_df['movie_id'].values.tolist()
    if len(movie_ids) > 0:
        movie_ids_str = ','.join([str(movie_id) for movie_id in movie_ids])
        movie_genres_count = netflix_pool.query_all_dict(
            "select movie_id,count(*) as movie_genres from netflix.genre_movie where movie_id in ({movie_ids_str}) group by movie_id".format(
                movie_ids_str=movie_ids_str))
        movie_genres_count_df = pandas.DataFrame(data=movie_genres_count)
        result = pandas.merge(movie_df, movie_genres_count_df, how='left', on=['movie_id'])
        return result.to_json(orient='records')

    return json.dumps(movie)


@api.route('/wordcount', methods=['GET'])
def movie_wordcount():
    movie_list = netflix_pool.query_column(
        "select concat(movie_en,', ',movie_cn) from netflix.movie;")
    movie_list = filter(None, movie_list)
    movie_list_str = ', '.join(movie_list)
    tags = jieba.analyse.extract_tags(movie_list_str, topK=100, withWeight=True)
    return json.dumps(tags)


@api.route('/genre_movie_relation', methods=['POST'])
def genre_movie_relation():
    movie_ids = request.json
    movie_ids_str = ','.join([str(movie_id) for movie_id in movie_ids])
    relations = netflix_pool.query_all_dict(
        "select g.genre_id,genre_en,m.movie_id,movie_en,img_en from netflix.genre_movie gm left join netflix.movie m on gm.movie_id=m.movie_id left join netflix.genre g on gm.genre_id=g.genre_id where m.movie_id in ({movie_ids_str})".format(
            movie_ids_str=movie_ids_str))
    return json.dumps(relations)
