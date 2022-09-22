import json
from googleapiclient.discovery import build
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pytrends.request import TrendReq
from datetime import datetime
import requests
import pytz

API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
YT_API = ''# Get key from https://developers.google.com/youtube/registering_an_application
YT_CHANNELS = "UC83jt4dlz1Gjl58fzQrrKZg,UC4p_I9eiRewn2KoU-nawrDg,UCNWh9GuY0APdpm6jwbX5JsA,UCpWvshQVx1d7BqCsPnVuNIw,UCsWp7U58TZM8uOYtRrAqWhg,UC9ULnkwvyofZ_sWrzRpyvrg"
YT_QUERY = "chicken AND (ban|export|price)"

GDRIVECRED = 'add_gdrive_credential.json'
SPREADSHEET = 'DAT2022_NearRealTime' #Create gsheet and share with account stated in Gdrive cred

GQUERY = 'chicken rice'

EVENTSTART = '2022-06-01'

NEWSAPI = ''# Get Key from https://newsapi.org/
NEWSQUERY = '""malaysia chicken"" AND export'


def create_yt_service(api_key):
    try:
        # Get credentials and create an API client
        service = build(API_SERVICE_NAME, API_VERSION, developerKey=api_key)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None


yt_service = create_yt_service(YT_API)


# define the scope
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name(GDRIVECRED, scope)

# authorize the clientsheet
client = gspread.authorize(creds)

# get the instance of the Spreadsheet
sheet = client.open(SPREADSHEET)


def runGoogleTrend():
    # get the first sheet of the Spreadsheet
    sheet_instance = sheet.get_worksheet(0)
    query = GQUERY
    startDate = EVENTSTART
    today = datetime.today().strftime('%Y-%m-%d')
    pytrends = TrendReq(hl='en-SG', tz=360)
    kw_list = [query]
    pytrends.build_payload(kw_list, timeframe='%s %s' %
                           (startDate, today), geo='SG', gprop='')
    df = pytrends.interest_over_time()
    df.reset_index(inplace=True)
    df['date'] = df['date'].astype(str)
    df['country'] = "SG"
    df.columns = map(lambda x: str(x).upper(), df.columns)

    pytrends = TrendReq(hl='en-SG', tz=360)
    kw_list = [query]
    pytrends.build_payload(kw_list, timeframe='%s %s' %
                           (startDate, today), geo='MY', gprop='')
    myDf = pytrends.interest_over_time()
    myDf.reset_index(inplace=True)
    myDf['date'] = myDf['date'].astype(str)
    myDf['country'] = "MY"
    myDf.columns = map(lambda x: str(x).upper(), myDf.columns)

    results = pd.concat([df, myDf])
    sheet_instance.update(
        [results.columns.values.tolist()] + results.values.tolist())


def runNews():
    sheet_instance = sheet.get_worksheet(1)
    query = NEWSQUERY
    api = NEWSAPI
    params = {'sortBy': 'publishedAt', 'q': query, 'apiKey': api}
    url = 'https://newsapi.org/v2/everything'
    resp = requests.get(url=url, params=params)
    data = resp.json()
    articles = pd.DataFrame(data['articles'])
    articles['source'] = articles['source'].str['name']
    print(articles)
    records_data = sheet_instance.get_all_records()
    combinedDf = pd.concat([articles, pd.DataFrame(records_data)]).drop_duplicates(subset=['source', 'title',
                                                                                           'description'], ignore_index=True).sort_values(by='publishedAt', ascending=False)
    sheet_instance.update(
        [combinedDf.columns.values.tolist()] + combinedDf.values.tolist())


def runYT():
    sheet_instance = sheet.get_worksheet(2)
    channelIds = YT_CHANNELS.split(',')
    startDate = EVENTSTART
    responseDfList = []
    for channelId in channelIds:
        response = yt_service.search().list(
            part="id, snippet",
            channelId=channelId,
            maxResults=50,
            order="date",
            publishedAfter=startDate + "T00:00:00Z",
            q='chicken AND (ban|export|price)',
            type="video"
        ).execute()

        if(len(response['items']) > 0):
            responseDfList.append(pd.DataFrame(pd.DataFrame(response['items'])['id'].str['videoId']).join(
                pd.DataFrame(list(pd.DataFrame(response['items'])['snippet']))))
        else:
            print(response)

    results = pd.concat(responseDfList).drop('thumbnails', axis=1)
    sheet_instance.update(
        [results.columns.values.tolist()] + results.values.tolist())




def lambda_handler(event, context):
    # TODO implement
    runGoogleTrend()
    runNews()
    runYT()
    sheet_instance = sheet.get_worksheet(3)
    sheet_instance.update_cell(2, 1, datetime.now(pytz.timezone('Singapore')).strftime('%Y-%m-%d %H:%M:%S')) 
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
