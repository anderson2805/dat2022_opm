from keybert import KeyBERT
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer
import spacy
#spacy.prefer_gpu()
nlp = spacy.load('en_core_web_lg')
#from keyphrase_vectorizers import KeyphraseCountVectorizer
#vectorizer = KeyphraseCountVectorizer(stop_words='english')
vectorizer = CountVectorizer(ngram_range=(1,1), stop_words='english')
#model = SentenceTransformer('all-MiniLM-L6-v2', device = 0)
#model = model.to("cuda:0")
kw_model = KeyBERT("all-mpnet-base-v2")



def checkChickenRiceTitle(text):
    if ("chicken" in text.lower() and "rice" in text.lower()):
        return True
    else:
        return False

def checkChickenRiceReview(text):
    if ("chicken rice" in text.lower()):
        return True
    else:
        return False

def textProcess(doc):
    if(doc is not None and doc[:5] == "(Tran"):
        doc = doc[23:]
        return doc[:doc.find('\n\n(Original)')]
    return doc


def extractTopics(classifier_pipeline, input_sequence, reviewId):
    label_candidate = ['price', 'portion', 'taste', 'service']
    result = classifier_pipeline(input_sequence, label_candidate)
    result['sequence'] = reviewId
    result['rank'] = [1,2,3,4]
    #result.rename({'sequence': 'reviewId'}, axis = 1, inplace = True)
    return result

def lemmatiser(doc):
    doc = nlp(str(doc)[1:-1])
    return set(" ".join([token.lemma_ for token in doc]).replace("'","").split(" , "))


def extractKeywords(doc):
    try:
        return lemmatiser(list(zip(*kw_model.extract_keywords(doc, vectorizer=vectorizer, 
                                stop_words='english', use_mmr=True)))[0])
    except (ValueError, IndexError):
        next