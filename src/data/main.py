from service import engine
from create import createJsonTable
from upload import uploadJson
from extract import extractPlace, extractReviewer, extractReview
from transform import transformMatched, transformText, transformReviewTopics, transformReviewKeywords
cursor = engine.connect()



if __name__ == '__main__':
    createJsonTable(cursor)
    uploadJson(cursor)
    extractPlace(cursor)
    extractReviewer(cursor)
    extractReview(cursor)
    transformMatched(engine)
    transformText(engine)
    transformReviewTopics(engine)
    transformReviewKeywords(engine)