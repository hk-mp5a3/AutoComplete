#!/usr/bin/python
# -*- coding: utf-8 -*-
from app.__init__ import db
db = db


class AutoComplete(db.Model):
    """Declare model for "auto_complete" database.
    
    +----------------+---------------+------+-----+---------+-------+
    | Field          | Type          | Null | Key | Default | Extra |
    +----------------+---------------+------+-----+---------+-------+
    | starting_words | varchar(3000) | YES  |     | NULL    |       |
    | following_word | varchar(3000) | YES  |     | NULL    |       |
    | word_count     | int(11)       | YES  |     | NULL    |       |
    +----------------+---------------+------+-----+---------+-------+
    
    Args:
        db (flask_sqlalchemy.model.DefaultMeta): base class of model
    """
    
    starting_words = db.Column(db.String(3000))
    following_word = db.Column(db.String(3000))
    word_count = db.Column(db.Integer, unique=False, nullable=True,
                           primary_key=True)
