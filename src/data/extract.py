#!/usr/bin/env python
import service
import logging
logging.getLogger().setLevel(logging.INFO)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

def extractPlace(cursor):
    result = cursor.execute(
        """create or replace table place(
            placeId text,
            title text, 
            categoryName text, 
            city text, 
            countryCode text, 
            address text, 
            locatedIn text, 
            neighborhood text, 
            street text, 
            postalCode INTEGER, 
            latitude double, 
            longitude double, 
            phone text, 
            reviewsCount number, 
            url text)
        as (SELECT DISTINCT src:placeId, src:title, src:categoryName, src:city,
        src:countryCode, src:address, src:locatedIn, src:neighborhood, src:street, 
        src:postalCode, src:location:lat, src:location:lng, replace(src:phone,' '), src:reviewsCount, src:url FROM RAW_TABLE QUALIFY ROW_NUMBER() OVER (PARTITION BY src:placeId ORDER BY src:placeId) = 1);
        """).fetchone()
    logging.info(result)

def extractReviewer(cursor):
    result = cursor.execute(
        """
    create or replace table reviewer(
        reviewerId text,
        isLocalGuide boolean, 
        reviewerNumberOfReviews number) as 
    (SELECT review.value:reviewerId,
    review.value:isLocalGuide,
    review.value:reviewerNumberOfReviews 
    FROM RAW_TABLE raw,
    table(flatten(src:reviews)) review 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY review.value:reviewerId ORDER BY review.value:reviewerNumberOfReviews desc) = 1);
        """
    ).fetchone()
    logging.info(result)


def extractReview(cursor):
    result = cursor.execute(
        """
    create or replace table review(
        placeId string,
        reviewId string, 
        publishedDate, 
        stars number, 
        text text, 
        likesCount number, 
        reviewerId string) as
        (SELECT DISTINCT raw.src:placeId, 
        review.value:reviewId, 
        to_timestamp(review.value:publishedAtDate::string, 'YYYY-MM-DDTHH24:MI:SS.FF3Z'), 
        review.value:stars,
        review.value:text, 
        review.value:likesCount, 
        review.value:reviewerId 
        FROM RAW_TABLE raw, 
        table(flatten(src:reviews)) review 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY review.value:reviewId ORDER BY review.value:reviewerNumberOfReviews desc) = 1);
        """
    ).fetchone()
    logging.info(result)

if __name__ == '__main__':
    cursor = service.engine.connect()
    extractPlace(cursor)
    extractReviewer(cursor)
    extractReview(cursor)