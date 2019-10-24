import logging
from saas import celery_app
import requests
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import asyncio
import aiohttp
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
from brand_safety.languages import LANGUAGES
from rest_framework.status import HTTP_429_TOO_MANY_REQUESTS

logger = logging.getLogger(__name__)

LOCK_NAME = 'custom_transcripts'

TASK_RETRY_TIME = 60
TASK_RETRY_COUNTS = 10


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_custom_transcripts_async(lang_codes, num_vids, num_runs):
    runs_counter = 0
    total_elapsed = 0
    soups_parsed = 0
    try:
        while runs_counter < num_runs:
            for lang_code in lang_codes:
                lang_lock = "{}__{}".format(LOCK_NAME, lang_code)
                lock(lock_name=lang_lock, max_retries=60, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
                init_es_connection()
                logger.debug("Pulling custom transcripts.")
                print("Pulling custom transcripts.")
                language = LANGUAGES[lang_code]
                unparsed_vids = get_unparsed_vids(language, num_vids)
                vid_ids = set([vid.main.id for vid in unparsed_vids])
                counter = 0
                transcripts_counter = 0
                video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                             upsert_sections=(Sections.CUSTOM_CAPTIONS,))
                start = time.perf_counter()
                all_video_soups_dict = asyncio.run(create_video_soups_dict(vid_ids, lang_code, total_elapsed, soups_parsed))
                for vid_id in vid_ids:
                    vid_obj = video_manager.get_or_create([vid_id])[0]
                    transcript_soup = all_video_soups_dict[vid_id]
                    transcript_text = replace_apostrophes(transcript_soup.text) if transcript_soup else ""
                    if transcript_text != "":
                        AuditVideoTranscript.get_or_create(video_id=vid_id, language=lang_code, transcript=str(transcript_soup))
                        logger.debug(f"VIDEO WITH ID {vid_id} HAS A CUSTOM TRANSCRIPT.")
                        print(f"VIDEO WITH ID {vid_id} HAS A CUSTOM TRANSCRIPT.")
                        transcripts_counter += 1
                    populate_video_custom_captions(vid_obj, [transcript_text], [lang_code])
                    video_manager.upsert([vid_obj])
                    counter += 1
                    logger.debug(f"Parsed video with id: {vid_id}")
                    logger.debug(f"Number of videos parsed: {counter}")
                    logger.debug(f"Number of transcripts retrieved: {transcripts_counter}")
                    print(f"Parsed video with id: {vid_id}")
                    print(f"Number of videos parsed: {counter}")
                    print(f"Number of transcripts retrieved: {transcripts_counter}")
                elapsed = time.perf_counter() - start
                total_elapsed += elapsed
                soups_parsed += len(all_video_soups_dict)
                logger.debug(f"Requested {num_vids} transcripts in {elapsed} seconds.")
                logger.debug(f"Total videos retrieved for {lang_code}: {num_vids}. Total time elapsed: {total_elapsed} seconds.")
                logger.debug(f"Finished Run #{runs_counter} of {num_runs} for {lang_code}.")
                print(all_video_soups_dict)
                print(f"Requested {num_vids} transcripts in {elapsed} seconds.")
                print(f"Total videos retrieved for {lang_code}: {num_vids}. Total time elapsed: {total_elapsed} seconds.")
                print(f"Finished Run #{runs_counter} of {num_runs} for {lang_code}.")
                unlock(lang_lock)
            runs_counter += 1
            print(f"Finished Run #{runs_counter} of {num_runs}, Starting Run #{runs_counter+1}.")
    except Exception:
        pass


async def create_video_soups_dict(vid_ids: set, lang_code: str, total_elapsed, soups_parsed):
    soups_dict = {}
    async with ClientSession() as session:
        await asyncio.gather(*[update_soup_dict(session, vid_id, lang_code, soups_dict, total_elapsed, soups_parsed) for vid_id in vid_ids])
    return soups_dict


async def update_soup_dict(session: ClientSession, vid_id: str, lang_code: str, soups_dict, total_elapsed, soups_parsed):
    transcript_url = "http://video.google.com/timedtext?lang={}&v=".format(lang_code)
    vid_transcript_url = transcript_url + vid_id
    counter = 0
    while counter < TASK_RETRY_COUNTS:
        try:
            transcript_response = await session.request(method="GET", url=vid_transcript_url)
            if transcript_response.status == HTTP_429_TOO_MANY_REQUESTS:
                await asyncio.sleep(TASK_RETRY_TIME)
                counter += 1
                print(f"Transcript request for video {vid_id} Attempt #{counter} of {TASK_RETRY_COUNTS} failed."
                      f"Sleeping for {TASK_RETRY_TIME} seconds."
                      f"Total soups retrieved before failure: {soups_parsed}."
                      f"Total time elapsed before: {total_elapsed} seconds.")
            else:
                break
        except HTTP_429_TOO_MANY_REQUESTS:
            await asyncio.sleep(TASK_RETRY_TIME)
            counter += 1
            print(f"Transcript request for video {vid_id} Attempt #{counter} of {TASK_RETRY_COUNTS} failed."
                  f"Sleeping for {TASK_RETRY_TIME} seconds."
                  f"Total soups retrieved before failure: {soups_parsed}."
                  f"Total time elapsed before: {total_elapsed} seconds.")
    if transcript_response.status == 200:
        soup = bs(await transcript_response.text(), "xml")
        soups_dict[vid_id] = soup
    else:
        soups_dict[vid_id] = None


def get_unparsed_vids(language, num_vids):
    s = Search(using='default')
    s = s.index(Video.Index.name)

    # Get Videos Query for Specified Language
    q1 = Q(
        {
            "term": {
                "general_data.language": {
                    "value": language
                }
            }
        }
    )
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
    s = s.query(q1).query(q2).query(q3)
    s = s.sort({"stats.views": {"order": "desc"}})
    s = s[:num_vids]
    return s.execute()
