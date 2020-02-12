import logging
import json
from saas import celery_app
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import time
import requests
from googleapiclient.discovery import build
from datetime import datetime
from django.utils import timezone
from datetime import timedelta
from datetime import time

from audit_tool.models import APIScriptTracker
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from utils.lang import replace_apostrophes
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from django.conf import settings
from transcripts.models import WatsonTranscript

logger = logging.getLogger(__name__)

LOCK_NAME = 'watson_transcripts'
API_KEY = settings.ESS_API_KEY
API_QUOTA = settings.WATSON_API_QUOTA
WATSON_APITRACKER_KEY = 'watson_transcripts'
batch_size = settings.WATSON_BATCH_SIZE
sandbox_mode = settings.WATSON_SANDBOX_MODE
youtube = build('youtube', 'v3', developerKey=settings.YOUTUBE_API_DEVELOPER_KEY)
watson_api_url = "https://api.essepi.io/transcribe/v1/prod"


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def submit_watson_transcripts():
    try:
        language = settings.WATSON_LANGUAGE
        country = settings.WATSON_COUNTRY
        yt_category = settings.WATSON_CATEGORY
        brand_safety_score = settings.WATSON_SCORE_THRESHOLD
        num_vids = settings.WATSON_NUM_VIDEOS
        logger.debug(f"language: {language}")
        logger.debug(f"county: {country}")
        logger.debug(f"yt_category: {yt_category}")
        logger.debug(f"brand_safety_score: {brand_safety_score}")
        logger.debug(f"num_vids: {num_vids}")
    except Exception as e:
        raise e
    vids_submitted = 0
    offset = 0
    try:
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        logger.debug("Starting submit_watson_transcripts task.")
        api_tracker = APIScriptTracker.objects.get_or_create(name=WATSON_APITRACKER_KEY)[0]
        # Get Videos in Elastic Search that have been parsed for Custom Captions but don't have any
        videos_to_upsert = []
        videos_request_batch = []
        videos_watson_transcripts = []
        manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, ),
                               upsert_sections=(Sections.CUSTOM_CAPTIONS, ))
        while vids_submitted < num_vids:
            videos = get_no_custom_captions_vids(language=language, country=country, yt_category=yt_category,
                                                 brand_safety_score=brand_safety_score, num_vids=num_vids,
                                                 offset=offset)
            offset += num_vids
            logger.debug(f"len(videos): {len(videos)}")
            for vid in videos:
                if api_tracker.cursor >= API_QUOTA:
                    now = datetime.now(tz=timezone.utc)
                    tomorrow = now.date() + timedelta(days=1)
                    timeout = (datetime.combine(tomorrow, time.min) - now).total_seconds()
                    unlock(LOCK_NAME)
                    lock(lock_name=LOCK_NAME, max_retries=0, expire=timeout)
                    api_tracker.cursor = 0
                    logger.debug(f"EXCEEDED {API_QUOTA} Watson API Requests today. Locking task for {timeout} seconds.")
                    return
                logger.debug(f"len(videos_request_batch): {len(videos_request_batch)}")
                if len(videos_request_batch) < batch_size:
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
                            logger.debug(f"Video with id {vid_id} has YT captions: {yt_captions['items']}. Skipping...")
                            yt_has_captions = True
                        # If YT API has no captions object for video, and we have no custom transcript for it, send to Watson
                        if not yt_has_captions:
                            try:
                                watson_transcript = WatsonTranscript.get_or_create(vid_id)
                                if watson_transcript.submitted:
                                    continue
                                else:
                                    videos_watson_transcripts.append(watson_transcript)
                                    videos_request_batch.append(vid_id)
                                    if not sandbox_mode:
                                        videos_to_upsert.append(vid)
                            except Exception as e:
                                logger.debug(e)
                                continue
                    except Exception as e:
                        logger.debug(e)
                        continue
                else:
                    api_endpoint = "/submitjob"
                    api_request = watson_api_url + api_endpoint
                    sandbox = sandbox_mode
                    url_list = [{"url": "https://www.youtube.com/watch?v="+vid_id} for vid_id in videos_request_batch]
                    request_body = {
                        "sandbox": sandbox,
                        "url_list": url_list
                    }
                    headers = {
                        "Content-Type": "application/json",
                        "x-api-key": API_KEY
                    }
                    logger.debug(f"Sending Watson Transcript /submitjob API request for {batch_size} videos.")
                    response = requests.post(api_request, data=json.dumps(request_body), headers=headers)
                    vids_submitted += batch_size
                    api_tracker.cursor += 1
                    api_tracker.save()
                    logger.debug(f"Submitted Watson Transcript /submitjob API request for {batch_size} videos.")
                    logger.debug(f"Response Status: {response.status_code}")
                    logger.debug(f"Response Content: {response.content}")
                    logger.debug(f"Watson API Requests submitted today: {api_tracker.cursor}")
                    job_id = response.json()["Job Id"]
                    for watson_transcript in videos_watson_transcripts:
                        watson_transcript.submitted = timezone.now()
                        watson_transcript.job_id = job_id
                        watson_transcript.save(update_fields=['submitted', 'job_id'])
                    videos_watson_transcripts = []
                    if not sandbox_mode:
                        for video in videos_to_upsert:
                            video.populate_custom_captions(watson_job_id=job_id)
                        manager.upsert(videos_to_upsert)
                        videos_to_upsert = []
                    videos_request_batch = []
        unlock(LOCK_NAME)
        logger.debug("Finished submitting Watson transcripts task.")
        logger.debug(f"Submitted {vids_submitted} video ids to Watson.")
    except Exception as e:
        logger.debug(e)
        pass


def get_no_custom_captions_vids(language=None, country=None, yt_category=None, brand_safety_score=None, num_vids=10000,
                                offset=0):
    forced_filters = VideoManager().forced_filters()
    s = Search(using='default')
    s = s.index(Video.Index.name)
    s = s.query(forced_filters)
    # Get Videos Query for Specified Language
    if language:
        language_query = Q(
            {
                "terms": {
                    "general_data.language": language
                }
            }
        )
    else:
        language_query = None
    # Get Videos Query for Specified Country
    if country:
        country_query = Q(
            {
                "terms": {
                    "general_data.country": country
                }
            }
        )
    else:
        country_query = None
    # Get Videos Query for Specified Category
    if yt_category:
        category_query = Q(
            {
                "terms": {
                    "general_data.category": yt_category
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

    # Get videos with no Watson Transcripts submitted
    no_watson_transcripts_query = Q(
        {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "custom_captions.watson_job_id"
                    }
                }
            }
        }
    )

    s = s.query(custom_captions_parsed_query).query(no_custom_captions_query).query(no_yt_captions_query) \
        .query(no_watson_transcripts_query)

    if language_query:
        s = s.query(language_query)
    if country_query:
        s = s.query(country_query)
    if category_query:
        s = s.query(category_query)
    if brand_safety_query:
        s = s.query(brand_safety_query)
    s = s.sort({"stats.views": {"order": "desc"}})
    s = s[offset:offset+num_vids]
    return s.execute()
