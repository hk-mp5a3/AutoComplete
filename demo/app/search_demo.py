#!/usr/bin/python
# -*- coding: utf-8 -*-
from app import app
from app import database
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for
import json


def sentence_preprocessing(sentence):
    """Prepossing the sentence for SQL query.
    
    Prepocessing the sentence for query:
    - Delete redundant space
    - Convert into lower case
    
    Args:
        sentence (str): the str to be processed
    
    Returns:
        str: the str after processing
    """

    while '  ' in sentence:
        sentence = sentence.replace('  ', ' ')
    if sentence[0] == ' ':
        sentence = sentence[1:]
    if sentence[-1] == ' ':
        sentence = sentence[:-1]
    sentence = sentence.lower()
    return sentence


@app.route('/')
def index():
    """Return the template of homepage
    
    Return the template "index.html" of homepage. It includes a search
    bar where the user can input any text and get search suggestions
    based on the corpus selected.
    
    Returns:
        HTML page: the homepage
    """

    return render_template('index.html')


@app.route('/search/autocomplete/', methods=['POST'])
def autocomplete():
    """Return autocomplete suggestion for search bar
    
    Send query to database and return the search suggestion for
    the search bar to autocomplete. 
    Request URL: /search/autocomplete/
    Method: POST
    Example Request Data: 
        {
            "starting_words": "white house"
        }
    Example Response Data:
        {
            "following_word": [
                "white house press",
                "white house and",
                "white house the",
                "white house officials",
                "white house to"
            ]
        }
    
    Returns:
        JSON: The response data
    """

    # Load request data
    data = json.loads(request.data)
    # Preprocess to get the starting words for query
    starting_words = data['starting_words']
    starting_words = sentence_preprocessing(starting_words)
    # Send query to database
    query_results = \
        database.AutoComplete.query.filter_by(starting_words=starting_words) \
        .order_by(database.AutoComplete.word_count.desc()).limit(10)
    # Process query result to get the final result
    following_word = []
    for query_result in query_results:
        following_word.append(query_result.starting_words + ' '
                              + query_result.following_word)
    # Construct JSON dict for response and send response
    response_dict = {'following_word': following_word}
    response = jsonify(response_dict)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
