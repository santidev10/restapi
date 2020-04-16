import logging
import json
import time
import asyncio
from aiohttp import ClientSession
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
from django.core.management import BaseCommand
from django.conf import settings
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from transcripts.utils import *


from pid import PidFile

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        with PidFile(piddir=".", pidname="pull_tts_url_transcripts.pid") as p:
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
            videos = self.get_no_transcripts_vids(lang_codes=lang_codes, country_codes=country_codes,
                                                  yt_categories=yt_categories, brand_safety_score=brand_safety_score,
                                                  num_vids=num_vids)

    # def pull_and_update_transcripts(self, unparsed_vids):
    #     video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
    #                                  upsert_sections=(Sections.CUSTOM_CAPTIONS,))
    #     transcripts_counter = 0
    #     vid_counter = 0
    #     vid_ids = set([vid.main.id for vid in unparsed_vids])
    #     start = time.perf_counter()
    #     # all_videos_lang_soups_dict = asyncio.run(create_video_soups_dict(vid_ids))
    #     all_videos = video_manager.get(list(vid_ids))
    #     for vid_obj in all_videos:
    #         vid_id = vid_obj.main.id
    #         transcripts_counter = parse_and_store_transcript_soups(vid_obj=vid_obj,
    #                                                                lang_codes_soups_dict=all_videos_lang_soups_dict[
    #                                                                    vid_id],
    #                                                                transcripts_counter=transcripts_counter)
    #         vid_counter += 1
    #         # logger.info(f"Parsed video with id: {vid_id}")
    #         # logger.info(f"Number of videos parsed: {vid_counter}")
    #         # logger.info(f"Number of transcripts retrieved: {transcripts_counter}")
    #     video_manager.upsert(all_videos)
    #     elapsed = time.perf_counter() - start
    #     logger.info(f"Upserted {len(all_videos)} videos in {elapsed} seconds.")
    #     rescore_brand_safety_videos.delay(vid_ids=list(vid_ids))

    @staticmethod
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
