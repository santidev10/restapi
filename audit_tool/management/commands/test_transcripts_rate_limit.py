import logging
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import asyncio
import aiohttp
from aiohttp import ClientSession
import time

from django.core.management.base import BaseCommand
from pid import PidFile
from es_components.connections import init_es_connection
from bs4 import BeautifulSoup as bs
from es_components.models.video import Video
from brand_safety.languages import LANGUAGES
from rest_framework.status import HTTP_429_TOO_MANY_REQUESTS

LOCK_NAME = 'custom_transcripts'

TASK_RETRY_TIME = 60
TASK_RETRY_COUNTS = 10


class Command(BaseCommand):
    def handle(self, *args, **options):
        with PidFile(piddir='.', pidname='test_transcripts_rate_limit.pid') as p:
            pull_transcripts(['en'], 1000, 100)


def pull_transcripts(lang_codes, num_vids, num_runs):
    runs_counter = 0
    total_elapsed = 0
    soups_parsed = 0
    while runs_counter < num_runs:
        for lang_code in lang_codes:
            init_es_connection()
            print("Pulling custom transcripts.")
            language = LANGUAGES[lang_code]
            unparsed_vids = get_unparsed_vids(language, num_vids)
            vid_ids = set([vid.main.id for vid in unparsed_vids])
            start = time.perf_counter()
            all_video_soups_dict = asyncio.run(create_video_soups_dict(vid_ids, lang_code, total_elapsed, soups_parsed))
            elapsed = time.perf_counter() - start
            total_elapsed += elapsed
            soups_parsed += len(all_video_soups_dict)
            print(all_video_soups_dict)
            print(f"Requested {num_vids} transcripts in {elapsed} seconds.")
            print(f"Total soups retrieved: {soups_parsed}. Total time elapsed: {total_elapsed} seconds.")
        runs_counter += 1
        print(f"Finished run {runs_counter} of {num_runs}")


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
