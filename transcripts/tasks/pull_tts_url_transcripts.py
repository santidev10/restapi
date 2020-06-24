import logging
import time
from datetime import timedelta

from celery.exceptions import Retry
from django.conf import settings
from django.core.exceptions import ValidationError
from elasticsearch_dsl import Q
from elasticsearch_dsl import Search
from requests.exceptions import ConnectionError as RequestsConnectionError

from audit_tool.models import AuditVideoTranscript
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from transcripts.tasks.rescore_brand_safety import rescore_brand_safety_videos
from transcripts.utils import YTTranscriptsScraper
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.transform import populate_video_custom_captions
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

LOCK_NAME = "tts_url_transcripts"


# pylint: disable=too-many-nested-blocks,too-many-statements,too-many-locals
@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_tts_url_transcripts():
    total_start = time.perf_counter()
    logger.info("Running pull_tts_url_transcripts...")
    try:
        lang_codes = settings.TRANSCRIPTS_LANG_CODES
        country_codes = settings.TRANSCRIPTS_COUNTRY_CODES
        iab_categories = settings.TRANSCRIPTS_CATEGORIES
        brand_safety_score = settings.TRANSCRIPTS_SCORE_THRESHOLD
        num_vids = settings.TRANSCRIPTS_NUM_VIDEOS
        logger.info("lang_codes: %s", lang_codes)
        logger.info("country_codes: %s", country_codes)
        logger.info("iab_categories: %s", iab_categories)
        logger.info("brand_safety_score: %s", brand_safety_score)
        logger.info("num_vids: %s", num_vids)
    # pylint: disable=broad-except
    except Exception as e:
        logger.error(e)
        raise e
    # pylint: enable=broad-except
    try:
        # pylint: disable=no-value-for-parameter
        lock(lock_name=LOCK_NAME, max_retries=1, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        # pylint: enable=no-value-for-parameter
        no_transcripts_query = get_no_transcripts_vids_query(lang_codes=lang_codes, country_codes=country_codes,
                                                             iab_categories=iab_categories,
                                                             brand_safety_score=brand_safety_score,
                                                             num_vids=num_vids)
        sort = [{"stats.views": {"order": "desc"}}]
        video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                     upsert_sections=(Sections.CUSTOM_CAPTIONS,))
        retrieval_start = time.perf_counter()
        all_videos = video_manager.search(query=no_transcripts_query, sort=sort, limit=num_vids).execute().hits
        retrieval_end = time.perf_counter()
        retrieval_time = retrieval_end - retrieval_start
        logger.info("Retrieved %s Videos from Elastic Search in %s seconds.", len(all_videos), retrieval_time)
        batch_size = settings.TRANSCRIPTS_BATCH_SIZE
        vid_ids_to_rescore = []
        for chunk in chunks_generator(all_videos, size=batch_size):
            videos_batch = list(chunk)
            vid_ids = list({vid.main.id for vid in videos_batch})
            transcripts_scraper = YTTranscriptsScraper(vid_ids=vid_ids)
            scraper_start = time.perf_counter()
            transcripts_scraper.run_scraper()
            scraper_end = time.perf_counter()
            scraper_time = scraper_end - scraper_start
            logger.info("Scraped %s Video Transcripts in %s seconds.", len(videos_batch), scraper_time)
            successful_vid_ids = list(transcripts_scraper.successful_vids.keys())
            vid_ids_to_rescore.extend(successful_vid_ids)
            logger.info(
                "Of %s videos, SUCCESSFULLY retrieved %s video transcripts, FAILED to retrieve %s video transcripts.",
                len(videos_batch), len(successful_vid_ids), transcripts_scraper.num_failed_vids)
            for vid_obj in videos_batch:
                vid_id = vid_obj.main.id
                if vid_id not in successful_vid_ids:
                    failure = transcripts_scraper.failure_reasons[vid_id]
                    if isinstance(failure, ValidationError) and failure.message == "No more proxies available.":
                        logger.info(failure.message)
                        logger.info("Locking pull_tts_url_transcripts task for 5 minutes.")
                        unlock(LOCK_NAME)
                        # pylint: disable=no-value-for-parameter
                        lock(lock_name=LOCK_NAME, max_retries=1, expire=timedelta(minutes=5).total_seconds())
                        # pylint: enable=no-value-for-parameter
                        raise Exception("No more proxies available. Locking pull_tts_url_transcripts task for 5 mins.")
                    if isinstance(failure, RequestsConnectionError):
                        continue
                    vid_obj.populate_custom_captions(transcripts_checked_tts_url=True)
                    continue
                vid_transcripts = [subtitle.captions for subtitle in
                                   transcripts_scraper.successful_vids[vid_id].subtitles]
                vid_lang_codes = [subtitle.lang_code for subtitle in
                                  transcripts_scraper.successful_vids[vid_id].subtitles]
                asr_lang = [subtitle.lang_code for subtitle in transcripts_scraper.successful_vids[vid_id].subtitles
                            if subtitle.is_asr]
                asr_lang = asr_lang[0] if asr_lang else None
                for i, item in enumerate(vid_transcripts):
                    if item:
                        AuditVideoTranscript.get_or_create(video_id=vid_id, language=vid_lang_codes[i],
                                                           transcript=item)
                populate_video_custom_captions(vid_obj, vid_transcripts, vid_lang_codes, source="tts_url",
                                               asr_lang=asr_lang)
            upsert_start = time.perf_counter()
            video_manager.upsert(videos_batch)
            upsert_end = time.perf_counter()
            upsert_time = upsert_end - upsert_start
            logger.info("Upserted %s Videos in %s seconds.", len(videos_batch), upsert_time)
        rescore_brand_safety_videos.delay(vid_ids=vid_ids_to_rescore)
        total_end = time.perf_counter()
        total_time = total_end - total_start
        logger.info("Parsed and stored %s Video Transcripts in %s seconds.", len(all_videos), total_time)
        unlock(LOCK_NAME)
        logger.info("Finished pulling TTS_URL transcripts task.")
    # pylint: disable=broad-except
    except Exception as e:
        if not isinstance(e, Retry):
            logger.error(e)
            if str(e) != "No more proxies available. Locking pull_tts_url_transcripts task for 5 mins.":
                unlock(LOCK_NAME)
    # pylint: enable=broad-except

# pylint: enable=too-many-nested-blocks,too-many-statements,too-many-locals

def get_no_transcripts_vids_query(lang_codes=None, country_codes=None, iab_categories=None, brand_safety_score=None,
                                  num_vids=10000):
    forced_filters = VideoManager().forced_filters()
    s = Search(using="default")
    s = s.index(Video.Index.name)
    s = s.query(forced_filters)
    # Get Videos Query for Specified Language
    if lang_codes:
        language_query = Q(
            {
                "terms": {
                    "general_data.lang_code": lang_codes
                }
            }
        )
    else:
        language_query = None
    # Get Videos Query for Specified Country
    if country_codes:
        country_query = Q(
            {
                "terms": {
                    "general_data.country_code": country_codes
                }
            }
        )
    else:
        country_query = None
    # Get Videos Query for Specified Category
    if iab_categories:
        category_query = Q(
            {
                "terms": {
                    "general_data.iab_categories": iab_categories
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

    # Get videos with no TTS_URL Transcripts submitted
    no_tts_url_checked_query = Q(
        {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "custom_captions.transcripts_checked_tts_url"
                    }
                }
            }
        }
    )

    query = custom_captions_parsed_query & no_custom_captions_query & no_yt_captions_query \
            & no_tts_url_checked_query

    if language_query:
        query &= language_query
    if country_query:
        query &= country_query
    if category_query:
        query &= category_query
    if brand_safety_query:
        query &= brand_safety_query
    return query
