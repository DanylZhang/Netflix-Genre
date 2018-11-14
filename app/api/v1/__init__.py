from flask import Blueprint

from app.api.v1 import user, client, genre, movie


def create_blueprint_v1():
    bp_v1 = Blueprint('v1', __name__)
    user.api.register(bp_v1)
    client.api.register(bp_v1)
    genre.api.register(bp_v1)
    movie.api.register(bp_v1)
    return bp_v1
