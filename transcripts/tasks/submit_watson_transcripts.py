import json
import logging
import time
from datetime import datetime
from datetime import time
from datetime import timedelta

import requests
from django.conf import settings
from django.utils import timezone
from elasticsearch_dsl import Q
from elasticsearch_dsl import Search
from googleapiclient.discovery import build

from audit_tool.models import APIScriptTracker
from audit_tool.models import AuditVideoTranscript
from audit_tool.models import get_hash_name
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from saas.urls.namespaces import Namespace
from transcripts.api.urls.names import TranscriptsPathName
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.unittests.reverse import reverse

logger = logging.getLogger(__name__)

LOCK_NAME = "watson_transcripts"
API_KEY = settings.ESS_API_KEY
API_QUOTA = settings.WATSON_API_QUOTA
WATSON_APITRACKER_KEY = "watson_transcripts"
batch_size = settings.WATSON_BATCH_SIZE
sandbox_mode = settings.WATSON_SANDBOX_MODE
watson_api_url = "https://api.essepi.io/transcribe/v1/prod"


# pylint: disable=too-many-locals,too-many-nested-blocks,too-many-nested-blocks,too-many-branches,too-many-statements
@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def submit_watson_transcripts():
    try:
        lang_codes = settings.WATSON_LANG_CODE
        countries = settings.WATSON_COUNTRY
        yt_categories = settings.WATSON_CATEGORY
        brand_safety_score = settings.WATSON_SCORE_THRESHOLD
        num_vids = settings.WATSON_NUM_VIDEOS
        logger.info("lang_code: %s", lang_codes)
        logger.info("county: %s", countries)
        logger.info("yt_category: %s", yt_categories)
        logger.info("brand_safety_score: %s", brand_safety_score)
        logger.info("num_vids: %s", num_vids)
    # pylint: disable=broad-except
    except Exception as e:
        # pylint: enable=broad-except
        raise e
    youtube = build("youtube", "v3", developerKey=settings.YOUTUBE_API_DEVELOPER_KEY, cache_discovery=False)
    vids_submitted = 0
    offset = 0
    try:
        # pylint: disable=no-value-for-parameter
        lock(lock_name=LOCK_NAME, max_retries=1, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        # pylint: enable=no-value-for-parameter
        logger.info("Starting submit_watson_transcripts task.")
        api_tracker = APIScriptTracker.objects.get_or_create(name=WATSON_APITRACKER_KEY)[0]
        # Get Videos in Elastic Search that have been parsed for Custom Captions but don't have any
        videos_request_batch = []
        videos_watson_transcripts = []
        manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                               upsert_sections=(Sections.CUSTOM_CAPTIONS,))
        while vids_submitted < num_vids:
            videos = get_no_custom_captions_vids(lang_code=lang_codes, country=countries, yt_category=yt_categories,
                                                 brand_safety_score=brand_safety_score, num_vids=num_vids,
                                                 offset=offset)
            offset += num_vids
            logger.info("len(videos): %s", len(videos))
            for vid in videos:
                if api_tracker.cursor >= API_QUOTA:
                    now = datetime.now()
                    tomorrow = now.date() + timedelta(days=1)
                    timeout = (datetime.combine(tomorrow, time.min) - now).total_seconds()
                    unlock(LOCK_NAME)
                    # pylint: disable=no-value-for-parameter
                    lock(lock_name=LOCK_NAME, max_retries=0, expire=timeout)
                    # pylint: enable=no-value-for-parameter
                    api_tracker.cursor = 0
                    api_tracker.save(update_fields=["cursor"])
                    logger.error("EXCEEDED %s Watson API Requests today. Locking task for %s seconds.",
                                 API_QUOTA, timeout)
                    return
                logger.info("len(videos_request_batch): %s", len(videos_request_batch))
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
                            logger.info("Video with id %s has YT captions: %s. Skipping...",
                                        vid_id, yt_captions["items"])
                            yt_has_captions = True
                        # If YT API has no captions object for video, and we have no custom transcript for it,
                        # send to Watson
                        if not yt_has_captions:
                            try:
                                lang_code = vid.general_data.lang_code
                                watson_transcript = AuditVideoTranscript.get_or_create(video_id=vid_id,
                                                                                       language=lang_code, source=1)
                                if watson_transcript.submitted:
                                    continue
                                videos_watson_transcripts.append(watson_transcript)
                                videos_request_batch.append(vid_id)
                            # pylint: disable=broad-except
                            except Exception as e:
                                logger.error(e)
                                continue
                            # pylint: enable=broad-except
                    # pylint: disable=broad-except
                    except Exception as e:
                        logger.error(e)
                        continue
                    # pylint: enable=broad-except
                else:
                    api_endpoint = "/submitjob"
                    api_request = watson_api_url + api_endpoint
                    sandbox = sandbox_mode
                    url_list = [{"url": "https://www.youtube.com/watch?v=" + vid_id} for vid_id in videos_request_batch]
                    callback_url = settings.HOST + \
                                   reverse(TranscriptsPathName.WATSON_TRANSCRIPTS, [Namespace.TRANSCRIPTS]) + \
                                   f"?authorization={settings.TRANSCRIPTS_API_TOKEN}"
                    request_body = {
                        "sandbox": sandbox,
                        "url_list": url_list,
                        "callback_url": callback_url
                    }
                    headers = {
                        "Content-Type": "application/json",
                        "x-api-key": API_KEY
                    }
                    logger.info("Sending Watson Transcript /submitjob API request for %s videos.", batch_size)
                    response = requests.post(api_request, data=json.dumps(request_body), headers=headers)
                    vids_submitted += batch_size
                    api_tracker.cursor += 1
                    api_tracker.save(update_fields=["cursor"])
                    logger.info("Submitted Watson Transcript /submitjob API request for %s videos.", batch_size)
                    logger.info("Response Status: %s", response.status_code)
                    logger.info("Response Content: %s", response.content)
                    logger.info("Watson API Requests submitted today: %s", api_tracker.cursor)
                    job_id = response.json()["Job Id"]
                    for watson_transcript in videos_watson_transcripts:
                        watson_transcript.submitted = timezone.now()
                        watson_transcript.job_id = job_id
                        watson_transcript.job_id_hash = get_hash_name(job_id)
                        watson_transcript.save(update_fields=["submitted", "job_id", "job_id_hash"])
                    videos_watson_transcripts = []
                    if not sandbox_mode:
                        videos_to_upsert = manager.get(videos_request_batch, skip_none=True)
                        for video in videos_to_upsert:
                            video.populate_custom_captions(watson_job_id=job_id)
                        manager.upsert(videos_to_upsert)
                        videos_to_upsert = []
                    videos_request_batch = []
        unlock(LOCK_NAME)
        logger.info("Finished submitting Watson transcripts task.")
        logger.info("Submitted %s video ids to Watson.", vids_submitted)
    # pylint: disable=broad-except
    except Exception as e:
        logger.error(e)
    # pylint: enable=broad-except

# pylint: enable=too-many-locals,too-many-nested-blocks,too-many-nested-blocks,too-many-branches,too-many-statements

def get_no_custom_captions_vids(lang_code=None, country=None, yt_category=None, brand_safety_score=None, num_vids=10000,
                                offset=0):
    forced_filters = VideoManager().forced_filters()
    s = Search(using="default")
    s = s.index(Video.Index.name)
    s = s.query(forced_filters)
    # Get Videos Query for Specified Language
    if lang_code:
        language_query = Q(
            {
                "terms": {
                    "general_data.lang_code": lang_code
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
    s = s[offset:offset + num_vids]
    return s.execute()
