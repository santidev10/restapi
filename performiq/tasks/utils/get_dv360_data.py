import csv
import io
import time
import pprint

from six.moves.urllib.request import urlopen
from django.conf import settings
from contextlib import closing
from performiq.utils.dv360 import load_credentials
from performiq.utils.dv360 import get_discovery_resource
from performiq.models import OAuthAccount
from googleapiclient import discovery
import httplib2
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import http_client
from googleapiclient import http as googleHttp
from google.oauth2 import service_account

from google.oauth2.credentials import Credentials


def get_data():
    account = OAuthAccount.objects.get(id=7)
    credentials = Credentials(
        account.token,
        refresh_token=account.refresh_token,
        token_uri="https://accounts.google.com/o/oauth2/token",
        client_id=settings.PERFORMIQ_OAUTH_CLIENT_ID,
        client_secret=settings.PERFORMIQ_OAUTH_CLIENT_SECRET,
    )
    service = discovery.build("doubleclickbidmanager", "v1.1", credentials=credentials)
    operation = service.queries().createquery(body=r).execute()
    query_request = service.queries().getquery(queryId=operation["queryId"])
    response = get_query_completion(query_request)
    report_url = response["metadata"]["googleCloudStoragePathForLatestReport"]
    with open("output", "wb") as file,\
            closing(urlopen(report_url)) as url:
        file.write(url.read())



r = {
    "params": {
        "type": "TYPE_GENERAL",
        "metrics": [
            "METRIC_IMPRESSIONS",
            "METRIC_CLICKS",
            "METRIC_CTR",
        ],
        "groupBys": [
            "FILTER_ADVERTISER",
            "FILTER_ADVERTISER_CURRENCY",
            "FILTER_YOUTUBE_CHANNEL",
        ],
        "filters": [
            {
                "type": "FILTER_ADVERTISER",
                "value": "1878225"
            },
            # {
            #     "type": "FILTER_MEDIA_PLAN",
            #     "value": "4146448"
            # }
        ],
    },
    "metadata": {
        "title": "DV360 Automation API-generated report",
        "dataRange": "LAST_90_DAYS",
        "format": "csv"
    },
    "schedule": {
        "frequency": "ONE_TIME"
    }
}

def get_query_completion(getquery_request):
    response = getquery_request.execute()
    pprint.pprint(response)
    while True:
        if not response["metadata"]["running"]:
            break
        print("The operation has not completed.")
        time.sleep(5)
        response = getquery_request.execute()
    return response


# res = resource.advertisers().creatives().list(advertiserId="6047131").execute()
    # task = {
    #     # "version": "",
    #     "advertiserId": "6047131",
    #     "parentEntityFilter": {
    #         "fileType": "FILE_TYPE_CAMPAIGN",
    #         "filterType": "FILTER_TYPE_NONE"
    #     }
    # }
    # operation = service.sdfdownloadtasks().create(body=task).execute()
    # operationName = operation["name"]
    # getRequest = service.sdfdownloadtasks().operations().get(name=operationName)
    # operation = getRequest.execute()
    # resourceName = operation["response"]["resourceName"]
    # downloadRequest = service.media().download_media(resourceName=operation["response"]["resourceName"])
    # outStream = io.FileIO("test2", mode="wb")
    # downloader = googleHttp.MediaIoBaseDownload(outStream, downloadRequest)
    # download_finished = False
    # while download_finished is False:
    #     _, download_finished = downloader.next_chunk()
    # return resource.advertisers().campaigns().list(advertiserId=str(advertiser.id)).execute()