import logging
from saas import celery_app
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import asyncio
import time
from aiohttp import ClientSession

from es_components.connections import init_es_connection
from bs4 import BeautifulSoup as bs
from audit_tool.models import AuditVideoTranscript
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from utils.transform import populate_video_custom_captions
from utils.lang import replace_apostrophes
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from brand_safety.languages import LANGUAGES, LANG_CODES
from aiohttp.web import HTTPTooManyRequests
from django.conf import settings

logger = logging.getLogger(__name__)

LOCK_NAME = 'custom_transcripts'

TASK_RETRY_TIME = 60
TASK_RETRY_COUNTS = 10


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_custom_transcripts():
    try:
        lang_codes = settings.CUSTOM_TRANSCRIPTS_LANGUAGES
    except Exception as e:
        lang_codes = ['en']
    try:
        num_vids = settings.CUSTOM_TRANSCRIPTS_RATE
    except Exception as e:
        num_vids = 1000
    total_elapsed = 0
    vid_counter = 0
    transcripts_counter = 0
    try:
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        init_es_connection()
        if lang_codes:
            for lang_code in lang_codes:
                logger.debug(f"Pulling {num_vids} '{lang_code}' custom transcripts.")
                unparsed_vids = get_unparsed_vids(lang_code, num_vids)
                vid_ids = set([vid.main.id for vid in unparsed_vids])
                video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                             upsert_sections=(Sections.CUSTOM_CAPTIONS,))
                start = time.perf_counter()
                all_video_soups_dict = asyncio.run(create_video_soups_dict(vid_ids, lang_code))
                all_videos = video_manager.get(list(vid_ids))
                for vid_obj in all_videos:
                    vid_id = vid_obj.main.id
                    transcript_soup = all_video_soups_dict[vid_id]
                    transcript_text = replace_apostrophes(transcript_soup.text).strip() if transcript_soup else ""
                    if transcript_text != "":
                        AuditVideoTranscript.get_or_create(video_id=vid_id, language=lang_code, transcript=str(transcript_soup))
                        logger.debug(f"VIDEO WITH ID {vid_id} HAS A CUSTOM TRANSCRIPT.")
                        transcripts_counter += 1
                    populate_video_custom_captions(vid_obj, [transcript_text], [lang_code])
                    vid_counter += 1
                    logger.debug(f"Parsed video with id: {vid_id}")
                    logger.debug(f"Number of videos parsed: {vid_counter}")
                    logger.debug(f"Number of transcripts retrieved: {transcripts_counter}")
                video_manager.upsert(all_videos)
                elapsed = time.perf_counter() - start
                total_elapsed += elapsed
                logger.debug(f"Upserted {len(all_videos)} '{lang_code}' videos in {elapsed} seconds.")
                logger.debug(f"Total number of videos retrieved so far: {vid_counter}. Total time elapsed: {total_elapsed} seconds.")
        else:
            logger.debug(f"Pulling {num_vids} custom transcripts.")
            unparsed_vids = get_unparsed_vids("", num_vids)
            vid_languages = {}
            for vid in unparsed_vids:
                if "general_data" in vid and "language" in vid.general_data:
                    vid_languages[vid.main.id] = vid.general_data.language
                else:
                    vid_languages[vid.main.id] = "English"
            vid_lang_codes = {}
            for vid_id in vid_languages:
                try:
                    vid_lang = vid_languages[vid_id]
                    lang_code = LANG_CODES[vid_lang]
                    vid_lang_codes[vid_id] = lang_code
                except Exception:
                    vid_lang_codes[vid_id] = 'en'
            vid_ids = {vid_id for vid_id in vid_lang_codes}
            video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                         upsert_sections=(Sections.CUSTOM_CAPTIONS,))
            start = time.perf_counter()
            all_video_soups_dict = asyncio.run(create_video_soups_dict_multi_lang(vid_lang_codes))
            all_videos = video_manager.get(list(vid_ids))
            for vid_obj in all_videos:
                vid_id = vid_obj.main.id
                lang_code = vid_lang_codes[vid_id]
                transcript_soup = all_video_soups_dict[vid_id]
                transcript_text = replace_apostrophes(transcript_soup.text).strip() if transcript_soup else ""
                if transcript_text != "":
                    AuditVideoTranscript.get_or_create(video_id=vid_id, language=lang_code,
                                                       transcript=str(transcript_soup))
                    logger.debug(f"VIDEO WITH ID {vid_id} HAS A CUSTOM TRANSCRIPT.")
                    transcripts_counter += 1
                populate_video_custom_captions(vid_obj, [transcript_text], [lang_code])
                vid_counter += 1
                logger.debug(f"Parsed video with id: {vid_id}")
                logger.debug(f"Number of videos parsed: {vid_counter}")
                logger.debug(f"Number of transcripts retrieved: {transcripts_counter}")
            video_manager.upsert(all_videos)
            elapsed = time.perf_counter() - start
            total_elapsed += elapsed
            logger.debug(f"Upserted {len(all_videos)} videos in {elapsed} seconds.")
        unlock(LOCK_NAME)
        logger.debug("Finished pulling custom transcripts task.")
    except Exception as e:
        pass


async def create_video_soups_dict(vid_ids: set, lang_code: str):
    soups_dict = {}
    async with ClientSession() as session:
        await asyncio.gather(*[update_soup_dict(session, vid_id, lang_code, soups_dict) for vid_id in vid_ids])
    return soups_dict


async def create_video_soups_dict_multi_lang(vids_lang_code_dict: dict):
    soups_dict = {}
    async with ClientSession() as session:
        await asyncio.gather(*[update_soup_dict(session, vid_id, vids_lang_code_dict[vid_id], soups_dict)
                               for vid_id in vids_lang_code_dict])
    return soups_dict


async def update_soup_dict(session: ClientSession, vid_id: str, lang_code: str, soups_dict):
    transcript_url = "http://video.google.com/timedtext?lang={}&v=".format(lang_code)
    vid_transcript_url = transcript_url + vid_id
    counter = 0
    while counter < TASK_RETRY_COUNTS:
        try:
            transcript_response = await session.request(method="GET", url=vid_transcript_url)
            if transcript_response.status == 429:
                await asyncio.sleep(TASK_RETRY_TIME)
                counter += 1
                logger.debug(f"Transcript request for video {vid_id} Attempt #{counter} of {TASK_RETRY_COUNTS} failed."
                      f"Sleeping for {TASK_RETRY_TIME} seconds.")
            else:
                break
        except HTTPTooManyRequests:
            await asyncio.sleep(TASK_RETRY_TIME)
            counter += 1
            logger.debug(f"Transcript request for video {vid_id} Attempt #{counter} of {TASK_RETRY_COUNTS} failed."
                  f"Sleeping for {TASK_RETRY_TIME} seconds.")
        except Exception as e:
            logger.debug(e)
            raise e
    if transcript_response.status == 200:
        soup = bs(await transcript_response.text(), "xml")
        soups_dict[vid_id] = soup
    else:
        soups_dict[vid_id] = None


def get_unparsed_vids(lang_code, num_vids):
    forced_filters = VideoManager().forced_filters()
    s = Search(using='default')
    s = s.index(Video.Index.name)
    s = s.query(forced_filters)
    # Get Videos Query for Specified Language
    if lang_code:
        q1 = Q(
            {
                "term": {
                    "general_data.lang_code": {
                        "value": lang_code
                    }
                }
            }
        )
    else:
        q1 = None
    # Get Videos with no captions
    q2 = Q(
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
    # Get Videos with no custom_captions.transcripts_checked
    q3 = Q(
        {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "custom_captions.transcripts_checked"
                    }
                }
            }
        }
    )
    if language:
        s = s.query(q1).query(q2).query(q3)
    else:
        s = s.query(q2).query(q3)
    s = s.sort({"stats.views": {"order": "desc"}})
    s = s[:num_vids]
    return s.execute()
