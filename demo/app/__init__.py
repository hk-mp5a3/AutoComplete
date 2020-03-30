#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
import config

app = Flask(__name__)

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://' \
    + config.user + ':' + config.passwd + '@' + config.host + ':' \
    + str(config.port) + '/' + config.db

db = SQLAlchemy(app)
Bootstrap(app)

from app import search_demo, database
