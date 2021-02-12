import logging
import time
from datetime import timedelta

from celery.exceptions import Retry
from django.conf import settings
from django.core.exceptions import ValidationError
from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Query
from typing import Type

from audit_tool.models import AuditVideoTranscript
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from transcripts.utils import YTTranscriptsScraper
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.transform import populate_video_custom_captions
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

LOCK_NAME = "tts_url_transcripts"


class NoMoreProxiesAvailableException(Exception):
    """ raised when transcripts tasks run out of proxies """
    pass


# pylint: disable=too-many-nested-blocks,too-many-statements,too-many-locals
@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_tts_url_transcripts_task():
    # TODO register the updated task name
    try:
        lang_codes = settings.TRANSCRIPTS_LANG_CODES
        country_codes = settings.TRANSCRIPTS_COUNTRY_CODES
        iab_categories = settings.TRANSCRIPTS_CATEGORIES
        brand_safety_score = settings.TRANSCRIPTS_SCORE_THRESHOLD
        num_vids = settings.TRANSCRIPTS_NUM_VIDEOS
    except Exception as e:
        logger.error(e)
        raise e
    logger.info(f"Running pull_tts_url_transcripts... \n"
                f"lang_codes: {lang_codes} \n"
                f"country_codes: {country_codes} \n"
                f"iab_categories: {iab_categories} \n"
                f"brand_safety_score: {brand_safety_score} \n"
                f"num_vids: {num_vids}")
    query = get_video_transcripts_query(lang_codes=lang_codes, country_codes=country_codes,
                                        iab_categories=iab_categories, brand_safety_score=brand_safety_score)
    pull_tts_url_transcripts_with_lock(lock_name=LOCK_NAME, query=query, num_vids=num_vids)


def pull_tts_url_transcripts_with_lock(lock_name: str, *args, **kwargs):
    """
    calls the pull_tts_url_transcripts_with_query function and locks
    :param lock_name: name of the lock to use for the task
    :param args:
    :param kwargs:
    :return:
    """
    lock(lock_name=lock_name, max_retries=1, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
    try:
        pull_tts_url_transcripts(*args, **kwargs)
    except Retry:
        pass
    except NoMoreProxiesAvailableException:
        logger.info("Locking pull_tts_url_transcripts task for 5 minutes.")
        unlock(lock_name)
        # pylint: disable=no-value-for-parameter
        lock(lock_name=lock_name, max_retries=1, expire=timedelta(minutes=5).total_seconds())
        logger.info("No more proxies available. Locking pull_tts_url_transcripts task for 5 mins.")
        # pylint: enable=no-value-for-parameter
    # pylint: disable=broad-except
    except Exception as e:
        unlock(lock_name)
        raise e
    # pylint: enable=broad-except


def pull_tts_url_transcripts(query: Type[Query], num_vids: int = settings.TRANSCRIPTS_NUM_VIDEOS):
    """
    Task for Pulling ASR Transcripts
    :param query: The query that will be used to select video records to be updated
    :param num_vids: number of videos to handle
    :return:
    """
    total_start = time.perf_counter()
    # pylint: disable=no-value-for-parameter
    sort = [{"brand_safety.overall_score": {"order": "desc"}}]
    video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY, Sections.TASK_US_DATA),
                                 upsert_sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY))
    # pylint: enable=no-value-for-parameter
    retrieval_start = time.perf_counter()
    all_videos = video_manager.search(query=query, sort=sort, limit=num_vids).execute().hits
    retrieval_end = time.perf_counter()
    retrieval_time = retrieval_end - retrieval_start
    logger.info("Retrieved %s Videos from Elastic Search in %s seconds.", len(all_videos), retrieval_time)
    batch_size = settings.TRANSCRIPTS_BATCH_SIZE
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
        vid_ids_to_rescore = []
        logger.info(f"Of {len(videos_batch)} videos, SUCCESSFULLY retrieved {len(successful_vid_ids)} video"
                    f" transcripts, FAILED to retrieve {transcripts_scraper.num_failed_vids} video transcripts.")
        updated_videos = []
        update_start = time.perf_counter()
        for vid_obj in videos_batch:
            vid_id = vid_obj.main.id
            if vid_id not in successful_vid_ids:
                failure = transcripts_scraper.failure_reasons[vid_id]
                if isinstance(failure, ValidationError) and failure.message == 'All proxies have been blocked.':
                    if updated_videos:
                        upsert_start = time.perf_counter()
                        video_manager.upsert(updated_videos)
                        upsert_end = time.perf_counter()
                        upsert_time = upsert_end - upsert_start
                        logger.info(f"Upserted {len(updated_videos)} Videos in {upsert_time} seconds.")
                    if vid_ids_to_rescore:
                        rescore_start = time.perf_counter()
                        rescore_filter = get_video_ids_query(vid_ids_to_rescore)
                        video_manager.update_rescore(filter_query=rescore_filter, rescore=True)
                        rescore_end = time.perf_counter()
                        rescore_time = rescore_end - rescore_start
                        logger.info(f"Updated {len(vid_ids_to_rescore)} Video IDs to be rescored in {rescore_time} "
                                    f"seconds.")
                    logger.info(failure.message)
                    transcripts_scraper.send_yt_blocked_email()
                    raise NoMoreProxiesAvailableException()
                if isinstance(failure, ConnectionError) or str(failure) == "Exceeded connection attempts to URL.":
                    continue
                else:
                    vid_obj.populate_custom_captions(transcripts_checked_tts_url=True)
                    updated_videos.append(vid_obj)
                    continue
            # we want to save the raw response to PG so that if we ever need to re-process
            # a transcript from the response, then we'll have it on hand
            raw_response = transcripts_scraper.successful_vids[vid_id].captions_url_response
            language = transcripts_scraper.successful_vids[vid_id].captions_language
            AuditVideoTranscript.get_or_create(video_id=vid_id, language=language, transcript=raw_response)
            # we'll store the processed transcript in ES for display
            processed_transcript = transcripts_scraper.successful_vids[vid_id].captions
            populate_video_custom_captions(vid_obj, [processed_transcript], [language], source="tts_url",
                                           asr_lang=language)
            updated_videos.append(vid_obj)
            if "task_us_data" not in vid_obj:
                vid_ids_to_rescore.append(vid_id)
        update_end = time.perf_counter()
        update_time = update_end - update_start
        logger.info(f"Populated Transcripts for {len(updated_videos)} Videos in {update_time} seconds.")
        upsert_start = time.perf_counter()
        video_manager.upsert(updated_videos)
        upsert_end = time.perf_counter()
        upsert_time = upsert_end - upsert_start
        logger.info(f"Upserted {len(updated_videos)} Videos in {upsert_time} seconds.")
        if vid_ids_to_rescore:
            rescore_start = time.perf_counter()
            rescore_filter = get_video_ids_query(vid_ids_to_rescore)
            video_manager.update_rescore(filter_query=rescore_filter, rescore=True)
            rescore_end = time.perf_counter()
            rescore_time = rescore_end - rescore_start
            logger.info(f"Updated {len(vid_ids_to_rescore)} Video IDs to be rescored in {rescore_time} seconds.")
    total_end = time.perf_counter()
    total_time = total_end - total_start
    logger.info("Parsed and stored %s Video Transcripts in %s seconds.", len(all_videos), total_time)
    logger.info("Finished pulling TTS_URL transcripts task.")


def get_video_ids_query(vid_ids):
    return Q(
        {
            "bool": {
                "must": {
                    "terms": {
                        "main.id": vid_ids
                    }
                }
            }
        }
    )


# pylint: enable=too-many-nested-blocks,too-many-statements,too-many-locals
def get_video_transcripts_query(lang_codes=None, country_codes=None, iab_categories=None, brand_safety_score=None):
    """ Generates query for retrieving all Videos in ElasticSearch that don't have ASR transcripts.

    Keyword arguments:
    lang_codes -- Language Codes of Videos to filter from ElasticSearch
    country_codes -- Country Codes of Videos to filter from ElasticSearch
    iab_categories -- IAB Categories of Videos to filter from ElasticSearch
    brand-safety_score -- Minimum Brand Safety Score of Videos to retrieve
    """
    query = Q()
    # Get Videos Query for Specified Language
    if lang_codes:
        query &= Q(
            {
                "terms": {
                    "general_data.lang_code": lang_codes
                }
            }
        )
    # Get Videos Query for Specified Country
    if country_codes:
        query &= Q(
            {
                "terms": {
                    "general_data.country_code": country_codes
                }
            }
        )
    # Get Videos Query for Specified Category
    if iab_categories:
        query &= Q(
            {
                "terms": {
                    "general_data.iab_categories": iab_categories
                }
            }
        )
    # Get Videos Query >= Brand Safety Score
    if brand_safety_score:
        query &= Q(
            {
                "range": {
                    "brand_safety.overall_score": {
                        "gte": brand_safety_score
                    }
                }
            }
        )
    # Set minimum views Get Videos Query >= stats.views
    query &= Q(
        {
            "bool": {
                "must": {
                    "range": {
                        "stats.views": {
                            "gte": 10000
                        }
                    }
                }
            }
        }
    )
    # Get Videos where Custom Captions have been parsed
    query &= Q(
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
    query &= Q(
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
    query &= Q(
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
    query &= Q(
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
    return query
