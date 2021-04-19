import logging
import time
from datetime import timedelta

from celery.exceptions import Retry
from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils import timezone
from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Query
from typing import Type

from administration.notifications import send_email
from audit_tool.models import APIScriptTracker
from audit_tool.models import AuditVideoTranscript
from es_components.constants import Sections
from es_components.managers.transcript import TranscriptManager
from es_components.managers.video import VideoManager
from es_components.models.transcript import Transcript
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from transcripts.constants import AuditVideoTranscriptSourceTypeIdEnum as SourceTypeIdEnum
from transcripts.constants import PROCESSOR_VERSION
from transcripts.constants import TranscriptSourceTypeEnum as SourceTypeEnum
from transcripts.utils import YTTranscriptsScraper
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.transform import populate_video_custom_captions
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

LOCK_NAME = "tts_url_transcripts"
TRANSCRIPTS_SUCCESS_COUNTER_NAME = "transcripts_tts_url_success_count"
TRANSCRIPTS_SUCCESS_COUNTER_DAYS = 1


class NoMoreProxiesAvailableException(Exception):
    """ raised when transcripts tasks run out of proxies """
    pass


# pylint: disable=too-many-nested-blocks,too-many-statements,too-many-locals
@celery_app.task(expires=TaskExpiration.TTS_URL_TRANSCRIPTS, soft_time_limit=TaskTimeout.TTS_URL_TRANSCRIPTS)
def pull_tts_url_transcripts_task():
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
    pull_tts_url_transcripts_with_lock(lock_name=LOCK_NAME, expire=TaskExpiration.TTS_URL_TRANSCRIPTS, query=query,
                                       num_vids=num_vids)


def pull_tts_url_transcripts_with_lock(lock_name: str, expire: int, *args, **kwargs):
    """
    calls the pull_tts_url_transcripts_with_query function and locks
    :param lock_name: name of the lock to use for the task
    :param expire: lock expiration time, in seconds
    :param args:
    :param kwargs:
    :return:
    """
    lock(lock_name=lock_name, max_retries=1, expire=expire)
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
    unlock(lock_name)


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
    success_count = 0
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
        success_count += len(successful_vid_ids)
        # vid_ids_to_rescore = []
        logger.info(f"Of {len(videos_batch)} videos, SUCCESSFULLY retrieved {len(successful_vid_ids)} video"
                    f" transcripts, FAILED to retrieve {transcripts_scraper.num_failed_vids} video transcripts.")
        updated_videos = []
        es_transcripts = []
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
                    # if vid_ids_to_rescore:
                    #     rescore_start = time.perf_counter()
                    #     rescore_filter = get_video_ids_query(vid_ids_to_rescore)
                    #     video_manager.update_rescore(filter_query=rescore_filter, rescore=True)
                    #     rescore_end = time.perf_counter()
                    #     rescore_time = rescore_end - rescore_start
                    #     logger.info(f"Updated {len(vid_ids_to_rescore)} Video IDs to be rescored in {rescore_time} "
                    #                 f"seconds.")
                    logger.info(failure.message)
                    transcripts_scraper.send_yt_blocked_email()
                    # store count of successes over a period of time, notify if none over that period
                    notify_if_no_successes()
                    update_successes_count(success_count)

                    raise NoMoreProxiesAvailableException()
                if isinstance(failure, ConnectionError) or str(failure) == "Exceeded connection attempts to URL.":
                    continue
                else:
                    vid_obj.populate_custom_captions(transcripts_checked_tts_url=True)
                    vid_obj.populate_brand_safety(rescore=True)
                    updated_videos.append(vid_obj)
                    continue
            # we want to save the raw response to PG so that if we ever need to re-process
            # a transcript from the response, then we'll have it on hand
            raw_response = transcripts_scraper.successful_vids[vid_id].captions_url_response
            lang_code = transcripts_scraper.successful_vids[vid_id].captions_language
            pg_transcript = AuditVideoTranscript.update_or_create_with_parent(
                video_id=vid_id, lang_code=lang_code, defaults={"source": SourceTypeIdEnum.TTS_URL.value,
                                                                "transcript": raw_response})
            # TODO for > 5.15: once we've completely migrated over to using the Transcripts index, we can remove this
            # we'll store the processed transcript in ES for display
            processed_text = transcripts_scraper.successful_vids[vid_id].captions
            populate_video_custom_captions(vid_obj, [processed_text], [lang_code], source="tts_url",
                                           asr_lang=lang_code)
            vid_obj.populate_brand_safety(rescore=True)
            updated_videos.append(vid_obj)
            # if "task_us_data" not in vid_obj:
            #     vid_ids_to_rescore.append(vid_id)

            # save ES Transcript record in the new Transcripts index, instead of on the video
            es_transcript = Transcript(pg_transcript.id)
            es_transcript.populate_video(id=vid_id)
            es_transcript.populate_text(value=processed_text)
            es_transcript.populate_general_data(language_code=lang_code, source_type=SourceTypeEnum.TTS_URL.value,
                                                is_asr=True, processor_version=PROCESSOR_VERSION,
                                                processed_at=timezone.now())
            es_transcripts.append(es_transcript)

        update_end = time.perf_counter()
        update_time = update_end - update_start
        logger.info(f"Populated Transcripts for {len(updated_videos)} Videos in {update_time} seconds.")
        upsert_start = time.perf_counter()
        video_manager.upsert(updated_videos, ignore_update_time_sections=[Sections.BRAND_SAFETY])
        transcript_manager = TranscriptManager(upsert_sections=(Sections.TEXT, Sections.VIDEO, Sections.GENERAL_DATA))
        transcript_manager.upsert(es_transcripts)
        upsert_end = time.perf_counter()
        upsert_time = upsert_end - upsert_start
        logger.info(f"Upserted {len(updated_videos)} Videos in {upsert_time} seconds.")
        # if vid_ids_to_rescore:
        #     rescore_start = time.perf_counter()
        #     rescore_filter = get_video_ids_query(vid_ids_to_rescore)
        #     video_manager.update_rescore(filter_query=rescore_filter, rescore=True)
        #     rescore_end = time.perf_counter()
        #     rescore_time = rescore_end - rescore_start
        #     logger.info(f"Updated {len(vid_ids_to_rescore)} Video IDs to be rescored in {rescore_time} seconds.")

    # store count of successes over a period of time, notify if none over that period
    notify_if_no_successes()
    update_successes_count(success_count)

    total_end = time.perf_counter()
    total_time = total_end - total_start
    logger.info("Parsed and stored %s Video Transcripts in %s seconds.", len(all_videos), total_time)
    logger.info("Finished pulling TTS_URL transcripts task.")


def notify_if_no_successes():
    """
    If there have been no successful transcript pulls in TRANSCRIPTS_SUCCESS_COUNTER_DAYS days, send an email
    :return:
    """
    now = timezone.now()
    counter, created = APIScriptTracker.objects.get_or_create(name=TRANSCRIPTS_SUCCESS_COUNTER_NAME,
                                                              defaults={"timestamp": now})
    if created:
        return

    # if monitoring period has not yet elapsed, do not notify
    delta = now - counter.timestamp
    if abs(delta.days) < TRANSCRIPTS_SUCCESS_COUNTER_DAYS:
        return

    # do not notify unless count is 0
    if counter.cursor > 0:
        return

    try:
        lock(lock_name="notify_if_no_daily_success_count", max_retries=1, expire=timedelta(hours=24).total_seconds())
    except Retry:
        return

    send_email(
        subject=f"Transcripts: no successful transcript pulls for {now.date()}",
        from_email=settings.SENDER_EMAIL_ADDRESS,
        recipient_list=["andrew.wong@channelfactory.com"],
        message=(
            f"There have been {counter.cursor} transcripts pulled successfully in the last {delta.days} days"
            f" ({delta.total_seconds()} total seconds)"
        )
    )


def notify_daily_total(counter: APIScriptTracker):
    """
    notify number of successful new transcripts pulled
    :param counter:
    :return:
    """
    send_email(
        subject=(f"Transcripts: {counter.cursor} transcripts pulled in the last {TRANSCRIPTS_SUCCESS_COUNTER_DAYS} "
                 f"day(s)"),
        from_email=settings.SENDER_EMAIL_ADDRESS,
        recipient_list=["andrew.wong@channelfactory.com"],
        message=(f"There have been {counter.cursor} transcripts pulled successfully in the last "
                 f"{TRANSCRIPTS_SUCCESS_COUNTER_DAYS} day(s)")
    )


def update_successes_count(count: int):
    """
    increment daily success count by `count`, or set to `count` and reset timestamp to now if monitoring period is over
    :param count:
    :return:
    """
    if not isinstance(count, int) or not count:
        return

    now = timezone.now()
    counter, created = APIScriptTracker.objects.get_or_create(name=TRANSCRIPTS_SUCCESS_COUNTER_NAME,
                                                              defaults={"timestamp": now, "cursor": count})
    if created:
        return

    # if monitoring period has elapsed, notify, and reset the timestamp and count
    delta = now - counter.timestamp
    if abs(delta.days) >= TRANSCRIPTS_SUCCESS_COUNTER_DAYS:
        notify_daily_total(counter)
        counter.timestamp = now
        counter.cursor = count
        counter.save(update_fields=["cursor", "timestamp"])
    # if within monitoring period, increment count
    else:
        counter.cursor += count
        counter.save(update_fields=["cursor"])


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
