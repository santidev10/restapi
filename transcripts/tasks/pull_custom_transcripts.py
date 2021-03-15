import asyncio
import logging
import time

from aiohttp import ClientSession
from aiohttp.web import HTTPTooManyRequests
from bs4 import BeautifulSoup as bs
from django.conf import settings
from elasticsearch_dsl import Q
from elasticsearch_dsl import Search

from audit_tool.constants import AuditVideoTranscriptSourceTypeEnum as SourceTypeEnum
from audit_tool.models import AuditVideoTranscript
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from es_components.connections import init_es_connection
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from transcripts.utils import get_formatted_captions_from_soup
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.transform import populate_video_custom_captions

logger = logging.getLogger(__name__)

LOCK_NAME = "custom_transcripts"

TASK_RETRY_TIME = 60
TASK_RETRY_COUNTS = 10


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def pull_custom_transcripts():
    try:
        lang_codes = settings.CUSTOM_TRANSCRIPTS_LANGUAGES
    # pylint: disable=broad-except
    except Exception:
        lang_codes = ["en"]
    # pylint: enable=broad-except

    try:
        num_vids = settings.CUSTOM_TRANSCRIPTS_RATE
    # pylint: disable=broad-except
    except Exception:
        num_vids = 1000
    # pylint: enable=broad-except

    try:
        # pylint: disable=no-value-for-parameter
        lock(lock_name=LOCK_NAME, max_retries=1, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
        # pylint: enable=no-value-for-parameter
        init_es_connection()
        if lang_codes:
            for lang_code in lang_codes:
                logger.info("Pulling %s '%s' custom transcripts.", num_vids, lang_code)
                unparsed_vids = get_unparsed_vids(lang_code=lang_code, num_vids=num_vids)
                pull_and_update_transcripts(unparsed_vids)
        else:
            logger.info("Pulling %s custom transcripts.", num_vids)
            unparsed_vids = get_unparsed_vids(num_vids=num_vids)
            pull_and_update_transcripts(unparsed_vids)
        unlock(LOCK_NAME)
        logger.info("Finished pulling custom transcripts task.")
    # pylint: disable=broad-except
    except Exception:
        pass
    # pylint: enable=broad-except


def pull_and_update_transcripts(unparsed_vids):
    video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.GENERAL_DATA),
                                 upsert_sections=(Sections.CUSTOM_CAPTIONS,))
    total_elapsed = 0
    transcripts_counter = 0
    vid_counter = 0
    vid_ids = {vid.main.id for vid in unparsed_vids}
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
    logger.info("Upserted %s videos in %s seconds.", len(all_videos), elapsed)


def parse_and_store_transcript_soups(vid_obj, lang_codes_soups_dict, transcripts_counter):
    transcript_texts = []
    lang_codes = []
    if not lang_codes_soups_dict:
        populate_video_custom_captions(vid_obj, transcript_texts, lang_codes, source="timedtext")
        return transcripts_counter
    vid_id = vid_obj.main.id
    top_5_transcripts = get_top_5_transcripts(lang_codes_soups_dict, vid_obj.general_data.lang_code)
    for vid_lang_code, transcript_soup in top_5_transcripts.items():
        transcript_text = get_formatted_captions_from_soup(transcript_soup)
        if transcript_text != "":
            AuditVideoTranscript.update_or_create_with_parent(video_id=vid_id, lang_code=vid_lang_code,
                                                              defaults={"source": SourceTypeEnum.CUSTOM.value,
                                                                        "transcript": str(transcript_soup)})
            logger.info("VIDEO WITH ID %s HAS A CUSTOM TRANSCRIPT.", vid_id)
            transcripts_counter += 1
            transcript_texts.append(transcript_text)
            lang_codes.append(vid_lang_code)
    populate_video_custom_captions(vid_obj, transcript_texts, lang_codes, source="timedtext")
    return transcripts_counter


def get_top_5_transcripts(transcripts_dict, video_lang_code):
    available_lang_codes = {key.split("-")[0].lower(): key for key in transcripts_dict}
    language_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY
    if video_lang_code not in language_priorities:
        language_priorities.insert(0, video_lang_code)
    top_5_transcripts = {}
    for lang_code in language_priorities:
        if len(top_5_transcripts) >= 5:
            return top_5_transcripts
        if lang_code in available_lang_codes:
            lang_code_key = available_lang_codes[lang_code]
            top_5_transcripts[lang_code] = transcripts_dict.pop(lang_code_key)
    for lang_code, transcript in transcripts_dict.items():
        if len(top_5_transcripts) >= 5:
            return top_5_transcripts
        cleaned_lang_code = lang_code.split("-")[0].lower()
        if cleaned_lang_code in top_5_transcripts:
            pass
        top_5_transcripts[cleaned_lang_code] = transcript
    return top_5_transcripts


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


# pylint: disable=too-many-nested-blocks
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
                    logger.debug("Transcript request for video %s Attempt #%s of %s failed. Sleeping for %s seconds.",
                                 vid_id, counter, TASK_RETRY_COUNTS, TASK_RETRY_TIME)
                else:
                    if transcript_response.status == 200:
                        soup = bs(await transcript_response.text(), "xml")
                        lang_code_soups_dict[lang_code] = soup
                    break
            except HTTPTooManyRequests:
                await asyncio.sleep(TASK_RETRY_TIME)
                counter += 1
                logger.debug("Transcript request for video %s Attempt #%s of %s failed. Sleeping for %s seconds.",
                             vid_id, counter, TASK_RETRY_COUNTS, TASK_RETRY_TIME)
            # pylint: disable=broad-except
            except Exception as e:
                logger.debug(e)
                raise e
            # pylint: enable=broad-except
    if lang_code_soups_dict:
        soups_dict[vid_id] = lang_code_soups_dict
    else:
        soups_dict[vid_id] = None
# pylint: enable=too-many-nested-blocks

def get_unparsed_vids(lang_code=None, num_vids=1000):
    forced_filters = VideoManager().forced_filters()
    s = Search(using="default")
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
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "custom_captions"
                                }
                            }
                        }
                    },
                    {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "custom_captions.transcripts_checked_v2"
                                }
                            }
                        }
                    }
                ]
            }
        }
    )
    if lang_code:
        s = s.query(q1 + q2 + q3 + q4)
    else:
        s = s.query(q2 + q3 + q4)
    s = s.sort({"stats.views": {"order": "desc"}})

    s = s[:num_vids]
    return s.execute()
