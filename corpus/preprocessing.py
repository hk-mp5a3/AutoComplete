#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd


def fetch_whole_data():
    """Extract all content and title from csv to txt file.
    
    The input files are csv which includes multiple columns 
    include title and content. In this function, we only select
    title and content and output to a txt file for data processing.
    Input File Names: articles[Number].csv
    Save to Files: input_articles[Number].txt
    """

    for index in range(1, 4):
        data = pd.read_csv('articles' + str(index) + '.csv')[['title',
                'content']]
        str_df = data.applymap(str)
        with open('input_articles' + str(index) + '.txt', 'w') as f:
            for (i, row) in str_df.iterrows():
                f.write(row['title'] + '\n')
                f.write(row['content'] + '\n')


def fetch_test_data():
    """Extract a few content and title from csv to txt file for testing.
    
    The input files are csv which includes multiple columns 
    include title and content. In this function, we only select a few
    title and content and output to a txt file for testing.
    Input File Names: articles1.csv
    Save to Files: testdata.txt
    """

    data = pd.read_csv('articles1.csv')[['title', 'content']]
    str_df = data.applymap(str)

    with open('testdata.txt', 'w') as f:
        row_count = 0
        for (i, row) in str_df.iterrows():
            if row_count > 1000:
                break
            f.write(row['title'] + '\n')
            f.write(row['content'] + '\n')
            row_count += 1
