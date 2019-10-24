import logging
from saas import celery_app
import requests
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import asyncio
import aiohttp
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

logger = logging.getLogger(__name__)

LOCK_NAME = 'custom_transcripts'


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_custom_transcripts(lang_codes):
    try:
        for lang_code in lang_codes:
            lang_lock = "{}__{}".format(LOCK_NAME, lang_code)
            lock(lock_name=lang_lock, max_retries=60, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
            init_es_connection()
            logger.debug("Pulling custom transcripts.")
            language = LANGUAGES[lang_code]
            unparsed_vids = get_unparsed_vids(language)
            vid_ids = set([vid.main.id for vid in unparsed_vids])
            counter = 0
            transcripts_counter = 0
            video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                         upsert_sections=(Sections.CUSTOM_CAPTIONS,))
            all_video_soups_dict = create_video_soups_dict(vid_ids, lang_code)
            for vid_id in vid_ids:
                vid_obj = video_manager.get_or_create([vid_id])[0]
                transcript_soup = all_video_soups_dict[vid_id]
                transcript_text = replace_apostrophes(transcript_soup.text) if transcript_soup else ""
                if transcript_text != "":
                    AuditVideoTranscript.get_or_create(video_id=vid_id, language=lang_code, transcript=str(transcript_soup))
                    logger.debug("VIDEO WITH ID {} HAS A CUSTOM TRANSCRIPT.".format(vid_id))
                    transcripts_counter += 1
                populate_video_custom_captions(vid_obj, [transcript_text], [lang_code])
                video_manager.upsert([vid_obj])
                counter += 1
                logger.debug("Parsed video with id: {}".format(vid_id))
                logger.debug("Number of videos parsed: {}".format(counter))
                logger.debug("Number of transcripts retrieved: {}".format(transcripts_counter))
            unlock(lang_lock)
            logger.debug("Finished pulling 1,000 {} custom transcripts.".format(lang_code))
    except Exception:
        pass


async def create_video_soups_dict(vid_ids: list, lang_code: str):
    soups_dict = {}
    async with ClientSession() as session:
        await asyncio.gather(*[update_soup_dict(vid_id, lang_code, session, soups_dict) for vid_id in vid_ids])
    return soups_dict


async def update_soup_dict(vid_id: str, lang_code: str, session: ClientSession, soups_dict):
    transcript_url = "http://video.google.com/timedtext?lang={}&v=".format(lang_code)
    vid_transcript_url = transcript_url + vid_id
    transcript_response = await session.request(method="GET", url=vid_transcript_url)
    if transcript_response.status == 200:
        soup = bs(await transcript_response.text(), "xml")
        soups_dict[vid_id] = soup
    else:
        soups_dict[vid_id] = None


# async def get_all_video_soups(vid_ids: list, lang_code: str):
#     async with ClientSession() as session:
#         all_soups = await asyncio.gather(*[get_video_soup(vid_id, lang_code, session) for vid_id in vid_ids])
#     return all_soups
#
#
# async def get_video_soup(vid_id: str, lang_code: str, session: ClientSession):
#     transcript_url = "http://video.google.com/timedtext?lang={}&v=".format(lang_code)
#     vid_transcript_url = transcript_url + vid_id
#     transcript_response = await session.request(method="GET", url=vid_transcript_url)
#     if transcript_response.status == 200:
#         soup = bs(await transcript_response.text(), "xml")
#         return soup
#     else:
#         return None


def get_unparsed_vids(language):
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
    s = s[:1000]
    return s.execute()
