import logging
from saas import celery_app
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import asyncio
import time
from aiohttp import ClientSession
from datetime import datetime
from datetime import timedelta

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
from aiohttp.web import HTTPTooManyRequests
from django.conf import settings
from transcripts.tasks.rescore_brand_safety import rescore_brand_safety_videos

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

    try:
        lock(lock_name=LOCK_NAME, max_retries=1, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        init_es_connection()
        if lang_codes:
            for lang_code in lang_codes:
                logger.info(f"Pulling {num_vids} '{lang_code}' custom transcripts.")
                unparsed_vids = get_unparsed_vids(lang_code=lang_code, num_vids=num_vids)
                pull_and_update_transcripts(unparsed_vids)
        else:
            logger.info(f"Pulling {num_vids} custom transcripts.")
            unparsed_vids = get_unparsed_vids(num_vids=num_vids)
            pull_and_update_transcripts(unparsed_vids)
        unlock(LOCK_NAME)
        logger.info("Finished pulling custom transcripts task.")
    except Exception as e:
        pass


def pull_and_update_transcripts(unparsed_vids):
    video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                 upsert_sections=(Sections.CUSTOM_CAPTIONS,))
    total_elapsed = 0
    transcripts_counter = 0
    vid_counter = 0
    vid_ids = set([vid.main.id for vid in unparsed_vids])
    start = time.perf_counter()
    all_videos_lang_soups_dict = asyncio.run(create_video_soups_dict(vid_ids))
    all_videos = video_manager.get(list(vid_ids))
    for vid_obj in all_videos:
        vid_id = vid_obj.main.id
        transcripts_counter = parse_and_store_transcript_soups(vid_obj=vid_obj,
                                         lang_codes_soups_dict=all_videos_lang_soups_dict[vid_id],
                                         transcripts_counter=transcripts_counter)
        vid_counter += 1
        # logger.info(f"Parsed video with id: {vid_id}")
        # logger.info(f"Number of videos parsed: {vid_counter}")
        # logger.info(f"Number of transcripts retrieved: {transcripts_counter}")
    video_manager.upsert(all_videos)
    elapsed = time.perf_counter() - start
    total_elapsed += elapsed
    logger.info(f"Upserted {len(all_videos)} videos in {elapsed} seconds.")
    rescore_brand_safety_videos.delay(vid_ids=list(vid_ids))


def parse_and_store_transcript_soups(vid_obj, lang_codes_soups_dict, transcripts_counter):
    if not lang_codes_soups_dict:
        return transcripts_counter
    vid_id = vid_obj.main.id
    transcript_texts = []
    lang_codes = []
    for vid_lang_code, transcript_soup in lang_codes_soups_dict.items():
        transcript_text = replace_apostrophes(transcript_soup.text).strip() if transcript_soup else ""
        if transcript_text != "":
            AuditVideoTranscript.get_or_create(video_id=vid_id, language=vid_lang_code,
                                               transcript=str(transcript_soup))
            logger.info(f"VIDEO WITH ID {vid_id} HAS A CUSTOM TRANSCRIPT.")
            transcripts_counter += 1
            transcript_texts.append(transcript_text)
            lang_codes.append(vid_lang_code)
    populate_video_custom_captions(vid_obj, transcript_texts, lang_codes, source="timedtext")
    return transcripts_counter


async def create_video_soups_dict(vid_ids: set):
    soups_dict = {}
    async with ClientSession() as session:
        await asyncio.gather(*[update_soup_dict(session, vid_id, soups_dict) for vid_id in vid_ids])
    return soups_dict


async def create_video_soups_dict_multi_lang(vids_lang_code_dict: dict):
    soups_dict = {}
    async with ClientSession() as session:
        await asyncio.gather(*[update_soup_dict(session, vid_id, soups_dict)
                               for vid_id in vids_lang_code_dict])
    return soups_dict


async def update_soup_dict(session: ClientSession, vid_id: str, soups_dict):
    lang_code_transcript_urls = {}
    lang_codes_url = f"http://video.google.com/timedtext?type=list&v={vid_id}"
    lang_codes_response = await session.request(method="GET", url=lang_codes_url)
    soup = bs(await lang_codes_response.text(), "xml")
    tracks = soup.find_all("track")
    for track in tracks:
        lang_code = track["lang_code"]
        name = track["name"]
        transcript_url = f"http://video.google.com/timedtext?lang={lang_code}&v={vid_id}"
        if name:
            transcript_url += f"&name={name}"
        lang_code_transcript_urls[lang_code] = transcript_url
    if not lang_code_transcript_urls:
        soups_dict[vid_id] = None
        return
    counter = 0
    lang_code_soups_dict = {}
    for lang_code, vid_transcript_url in lang_code_transcript_urls.items():
        while counter < TASK_RETRY_COUNTS:
            try:
                transcript_response = await session.request(method="GET", url=vid_transcript_url)
                if transcript_response.status == 429:
                    await asyncio.sleep(TASK_RETRY_TIME)
                    counter += 1
                    logger.debug(f"Transcript request for video {vid_id} Attempt #{counter} of {TASK_RETRY_COUNTS} failed."
                                 f"Sleeping for {TASK_RETRY_TIME} seconds.")
                else:
                    if transcript_response.status == 200:
                        soup = bs(await transcript_response.text(), "xml")
                        lang_code_soups_dict[lang_code] = soup
                    break
            except HTTPTooManyRequests:
                await asyncio.sleep(TASK_RETRY_TIME)
                counter += 1
                logger.debug(f"Transcript request for video {vid_id} Attempt #{counter} of {TASK_RETRY_COUNTS} failed."
                      f"Sleeping for {TASK_RETRY_TIME} seconds.")
            except Exception as e:
                logger.debug(e)
                raise e
    if lang_code_soups_dict:
        soups_dict[vid_id] = lang_code_soups_dict
    else:
        soups_dict[vid_id] = None


def get_unparsed_vids(lang_code=None, num_vids=1000):
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
    # Get Videos with no custom_captions.items field
    q3 = Q(
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
    # Get Videos that haven't had custom_captions updated in over a month
    q4 = Q(
        {
            "bool": {
                "should": [
                    {
                      "range": {
                        "custom_captions.updated_at": {
                          "lte": datetime.now() - timedelta(days=settings.CUSTOM_TRANSCRIPTS_OUTDATED_AT)
                        }
                      }
                    },
                    {
                      "bool": {
                        "must_not": {
                          "exists": {
                            "field": "custom_captions"
                          }
                        }
                      }
                    }
                ]
            }
        }
    )
    if lang_code:
        s = s.query(q1+q2+q3+q4)
    else:
        s = s.query(q2+q3+q4)
    s = s.sort({"stats.views": {"order": "desc"}})

    s = s[:num_vids]
    return s.execute()
