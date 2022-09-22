#!/usr/bin/env python
import sys
sys.path.append('src')
from utils import fix_date_cols
from transformers import pipeline
from snowflake.connector.pandas_tools import pd_writer
from models.features import checkChickenRiceTitle, checkChickenRiceReview, textProcess, extractTopics, extractKeywords
from tqdm import tqdm
import pandas as pd
import logging
from service import engine


logging.getLogger().setLevel(logging.INFO)
tqdm.pandas()
# https://docs.snowflake.com/en/user-guide/python-connector-example.html


def transformMatched(engine):
    """
    To confirm the place is "Chicken Rice", by checking title and reviews of place, that "chicken" and "rice" is mentioned.

    Args:
        cursor (_type_): _description_
    """
    cursor = engine.connect()
    placeDf = pd.read_sql_query(
        "SELECT placeid, title FROM place;", engine)
    reviewDf = pd.read_sql_query(
        "SELECT placeid, text FROM review where text != 'None' ;", engine)
    matchPlaceDf = pd.DataFrame(placeDf.set_index(
        'placeid').title.progress_apply(checkChickenRiceTitle)).reset_index()
    matchReviewDf = pd.DataFrame(reviewDf.text.progress_apply(
        checkChickenRiceReview).groupby(reviewDf.placeid).max()).reset_index()
    matchDf = matchPlaceDf.merge(right=matchReviewDf, on='placeid', how='left')
    matchDf['matched'] = matchDf['title'] | matchDf['text']
    matchDf.drop(labels=['title', 'text'], axis=1, inplace=True)
    matchDf.columns = map(lambda x: str(x).upper(), matchDf.columns)
    matchDf.to_sql(name="temp_match", con=engine,
                   if_exists='replace', method=pd_writer, index=False)
    result = cursor.execute(
        """create or replace table place as (select place.*, temp_match.matched from place, temp_match where place.placeid = temp_match.placeid);
        """).fetchone()
    logging.info(result)
    result = cursor.execute(
        """drop table temp_match;
    """).fetchone()
    logging.info(result)

def transformText(engine):
    reviewDf = pd.read_sql_query(
        "SELECT * FROM review;", engine)
    reviewDf['processed'] = reviewDf.text.progress_apply(textProcess)
    reviewDf['publisheddate'] = reviewDf.publisheddate.dt.tz_localize('Asia/Singapore')
    reviewDf.columns = map(lambda x: str(x).upper(), reviewDf.columns)
    reviewDf.to_sql(
        name="review_processed", con=engine, if_exists='replace', method=pd_writer, index=False)
    return True

def transformReviewTopics(engine):
    reviewDf = pd.read_sql_query(
        "SELECT reviewId, processed FROM review_processed where processed != 'None' ;", engine)
    classifier_pipeline = pipeline(
        "zero-shot-classification", model='facebook/bart-large-mnli', device=0)
    results = []
    for index, review in tqdm(reviewDf.iterrows(), total=reviewDf.shape[0]):
        results.append(extractTopics(classifier_pipeline,
                       review.processed, review.reviewid))
    resultDf = pd.DataFrame(results).rename({'sequence': 'reviewId'}, axis=1)
    resultDf.columns = map(lambda x: str(x).upper(), resultDf.columns)
    resultDf.explode(['LABELS', 'SCORES', 'RANK']).to_sql(
        name="review_topics", con=engine, if_exists='replace', method=pd_writer, index=False)
    return True


def transformReviewKeywords(engine):
    reviewDf = pd.read_sql_query(
        "SELECT reviewId, processed FROM review_processed where processed != 'None' ;", engine)
    reviewDf["keywords"] = reviewDf.processed.progress_apply(extractKeywords)
    resultDf = reviewDf[['reviewid', 'keywords']].explode('keywords')
    resultDf.columns = map(lambda x: str(x).upper(), resultDf.columns)
    resultDf.to_sql(
        name="review_keywords", con=engine, if_exists='replace', method=pd_writer, index=False)

if __name__ == '__main__':
    # transformMatched(engine)
    # transformText(engine)
    # transformReviewTopics(engine)
    transformReviewKeywords(engine)