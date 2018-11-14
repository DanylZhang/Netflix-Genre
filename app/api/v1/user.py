from flask import Blueprint

# blueprint
from app.libs.redprint import Redprint

user = Blueprint('user', __name__)
api = Redprint('user')


@api.route('/get')
def get_user():
    return "I'm danyl"
