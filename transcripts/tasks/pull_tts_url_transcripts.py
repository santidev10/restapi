import logging
import time
import asyncio
from saas import celery_app
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
from django.core.management import BaseCommand
from django.conf import settings
from audit_tool.models import AuditVideoTranscript
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from utils.transform import populate_video_custom_captions
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from transcripts.utils import YTTranscriptsScraper, LOCK_NAME

logger = logging.getLogger(__name__)


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_asr_transcripts():
    try:
        lang_codes = settings.TRANSCRIPTS_LANG_CODES
        country_codes = settings.TRANSCRIPTS_COUNTRY_CODES
        yt_categories = settings.TRANSCRIPTS_CATEGORIES
        brand_safety_score = settings.TRANSCRIPTS_SCORE_THRESHOLD
        num_vids = settings.TRANSCRIPTS_NUM_VIDEOS
        logger.info(f"lang_codes: {lang_codes}")
        logger.info(f"country_codes: {country_codes}")
        logger.info(f"yt_categories: {yt_categories}")
        logger.info(f"brand_safety_score: {brand_safety_score}")
        logger.info(f"num_vids: {num_vids}")
    except Exception as e:
        logger.error(e)
        raise e
    try:
        lock(lock_name=LOCK_NAME, max_retries=1, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        unparsed_vids = get_no_transcripts_vids(lang_codes=lang_codes, country_codes=country_codes,
                                         yt_categories=yt_categories, brand_safety_score=brand_safety_score,
                                         num_vids=num_vids)
        video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                     upsert_sections=(Sections.CUSTOM_CAPTIONS,))
        vid_ids = list(set([vid.main.id for vid in unparsed_vids]))
        start = time.perf_counter()
        transcripts_scraper = YTTranscriptsScraper(vid_ids=vid_ids)
        transcripts_scraper.run_scraper()
        successful_vid_ids = list(transcripts_scraper.successful_vids.keys())
        logger.info(f"Of {len(vid_ids)} videos, SUCCESSFULLY retrieved {len(successful_vid_ids)} video transcripts, "
                    f"FAILED to retrieve {transcripts_scraper.num_failed_vids} video transcripts.")
        successful_videos = video_manager.get(successful_vid_ids)
        for vid_obj in successful_videos:
            vid_id = vid_obj.main.id
            vid_transcripts = [subtitle.captions for subtitle in transcripts_scraper.successful_vids[vid_id].subtitles]
            vid_lang_codes = [subtitle.lang_code for subtitle in transcripts_scraper.successful_vids[vid_id].subtitles]
            asr_lang = [subtitle.lang_code for subtitle in transcripts_scraper.successful_vids[vid_id].subtitles
                        if subtitle.is_asr]
            asr_lang = asr_lang if asr_lang else None
            for i in range(len(vid_transcripts)):
                if vid_transcripts[i]:
                    AuditVideoTranscript.get_or_create(video_id=vid_id, language=vid_lang_codes[i],
                                                       transcript=vid_transcripts[i])
            populate_video_custom_captions(vid_obj, vid_transcripts, vid_lang_codes, source="tts_url", asr_lang=asr_lang)
        video_manager.upsert(successful_videos)
        elapsed = time.perf_counter() - start
        logger.info(f"Upserted {len(successful_videos)} videos in {elapsed} seconds.")
        unlock(LOCK_NAME)
        logger.info("Finished pulling ASR transcripts task.")
    except Exception as e:
        pass






def get_no_transcripts_vids(lang_codes=None, country_codes=None, yt_categories=None, brand_safety_score=None,
                            num_vids=10000):
    forced_filters = VideoManager().forced_filters()
    s = Search(using='default')
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
    if yt_categories:
        category_query = Q(
            {
                "terms": {
                    "general_data.category": yt_categories
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

    s = s.query(custom_captions_parsed_query).query(no_custom_captions_query).query(no_yt_captions_query) \
        .query(no_tts_url_checked_query)

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
