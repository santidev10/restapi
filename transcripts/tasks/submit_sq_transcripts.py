import logging
import json
from saas import celery_app
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import time
import requests
from googleapiclient.discovery import build
from datetime import datetime
from datetime import timedelta
from datetime import time

from audit_tool.models import APIScriptTracker
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from utils.lang import replace_apostrophes
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from django.conf import settings
from transcripts.models import SQTranscript

logger = logging.getLogger(__name__)

LOCK_NAME = 'sq_transcripts'
API_KEY = settings.SQ_API_KEY
API_QUOTA = settings.SQ_API_QUOTA
SQ_APITRACKER_KEY = 'sq_transcripts'
youtube = build('youtube', 'v3', developerKey=settings.YOUTUBE_API_DEVELOPER_KEY)
sq_api_url = "https://api.essepi.io/transcribe/v1/prod"


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def submit_sq_transcripts(language="English", country="United States", yt_category="News & Politics",
                          brand_safety_score=70, num_vids=10000):
    try:
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        api_tracker = APIScriptTracker.objects.get_or_create(name=SQ_APITRACKER_KEY)[0]
        # Get Videos in Elastic Search that have been parsed for Custom Captions but don't have any
        videos = get_no_custom_captions_vids(language=language, country=country, yt_category=yt_category,
                                             brand_safety_score=brand_safety_score, num_vids=num_vids)
        videos_request_batch = []
        videos_sq_transcripts = []
        for vid in videos:
            if api_tracker.cursor >= API_QUOTA:
                now = datetime.now()
                tomorrow = now.date() + timedelta(days=1)
                timeout = (datetime.combine(tomorrow, time.min) - now).total_seconds()
                unlock(LOCK_NAME)
                lock(lock_name=LOCK_NAME, max_retries=0, expire=timeout)
                api_tracker.cursor = 0
                return
            if len(videos_request_batch) < 100:
                vid_id = vid.main.id
                options = {
                    "part": "id,snippet",
                    "videoId": vid_id
                }
                try:
                    # Check if YT API contains a captions object for the Video.
                    yt_captions = youtube.captions().list(**options).execute()
                    if len(yt_captions["items"]) < 1:
                        yt_has_captions = False
                    else:
                        yt_has_captions = True
                    # If YT API has no captions object for video, and we have no custom transcript for it, send to SQ
                    if not yt_has_captions:
                        try:
                            sq_transcript = SQTranscript.get_or_create(vid_id)
                            if sq_transcript.submitted:
                                continue
                            else:
                                videos_sq_transcripts.append(sq_transcript)
                                videos_request_batch.append(vid_id)
                        except Exception:
                            continue
                except Exception as e:
                    continue
            else:
                api_endpoint = "/submitjob"
                api_request = sq_api_url + api_endpoint
                request_body = [{"url": "https://www.youtube.com/watch?v="+vid_id} for vid_id in videos_request_batch]
                headers = {
                    "Content-Type": "application/json",
                    "x-api-key": API_KEY
                }
                response = requests.post(api_request, data=json.dumps(request_body), headers=headers)
                api_tracker.cursor += 1
                api_tracker.save()
                for sq_transcript in videos_sq_transcripts:
                    sq_transcript.submitted = datetime.now()
                    sq_transcript.job_id = response.json()["Job Id"]
                    sq_transcript.save()
                videos_request_batch = []
        unlock(LOCK_NAME)
        logger.debug("Finished pulling SQ transcripts task.")
    except Exception as e:
        pass


def get_no_custom_captions_vids(language=None, country=None, yt_category=None, brand_safety_score=None, num_vids=10000):
    forced_filters = VideoManager().forced_filters()
    s = Search(using='default')
    s = s.index(Video.Index.name)
    s = s.query(forced_filters)
    # Get Videos Query for Specified Language
    if language:
        language_query = Q(
            {
                "term": {
                    "general_data.language": {
                        "value": language
                    }
                }
            }
        )
    else:
        language_query = None
    # Get Videos Query for Specified Country
    if country:
        country_query = Q(
            {
                "term": {
                    "general_data.country": {
                        "value": country
                    }
                }
            }
        )
    else:
        country_query = None
    # Get Videos Query for Specified Category
    if yt_category:
        category_query = Q(
            {
                "term": {
                    "general_data.category": {
                        "value": yt_category
                    }
                }
            }
        )
    else:
        category_query = None
    # Get Videos Query >= Brand Safety Score
    if brand_safety_score:
        brand_safety_query = Q(
            {
                "range": {
                    "brand_safety.overall_score": {
                        "gte": brand_safety_score
                    }
                }
            }
        )
    else:
        brand_safety_query = None

    # Get Videos where Custom Captions have been parsed
    custom_captions_parsed_query = Q(
        {
            "bool": {
                "must": {
                    "exists": {
                        "field": "custom_captions"
                    }
                }
            }
        }
    )

    # Get Videos with no Custom Captions, after parsing
    no_custom_captions_query = Q(
        {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "custom_captions.items"
                    }
                }
            }
        }
    )

    # Get Videos with no Captions
    no_yt_captions_query = Q(
        {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "captions"
                    }
                }
            }
        }
    )

    s = s.query(custom_captions_parsed_query).query(no_custom_captions_query).query(no_yt_captions_query)

    if language_query:
        s = s.query(language_query)
    if country_query:
        s = s.query(country_query)
    if category_query:
        s = s.query(category_query)
    if brand_safety_query:
        s = s.query(brand_safety_query)
    s = s.sort({"stats.views": {"order": "desc"}})
    s = s[:num_vids]
    return s.execute()
