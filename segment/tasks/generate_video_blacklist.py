import concurrent.futures
import csv
import logging
import os
import tempfile

from django.conf import settings

from es_components.constants import Sections
from es_components.models import Video
from es_components.query_builder import QueryBuilder
from utils.lang import merge
from utils.utils import chunks_generator


logger = logging.getLogger(__name__)


def generate_videos_blacklist(channel_ctl, channel_ids):
    all_videos = []
    video_blacklist_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        for chunk in chunks_generator(channel_ids, size=2):
            videos = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(get_videos_for_channel, channel_id, 80) for channel_id in chunk]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    videos.extend(result)
            videos.sort(key=lambda doc: doc.brand_safety.overall_score)
            all_videos = merge(all_videos, videos, lambda doc: doc.brand_safety.overall_score)

        rows = []
        for video in all_videos:
            row = [video.general_data.title]
            overall_score = video.brand_safety.overall_score if video.custom_properties.blocklist is False else -1
            row.append(overall_score)
            rows.append(row)

        with open(video_blacklist_fp, mode="w+") as file:
            writer = csv.writer(file)
            writer.writerow(["title", "score"])
            writer.writerows(rows)
        channel_ctl.s3.export_file_to_s3()
    except Exception:
        logger.exception(f"Uncaught exception for generate_videos_blacklist({channel_ctl, channel_ids})")
    finally:
        os.remove(video_blacklist_fp)


def get_videos_for_channel(channel_id, bs_score_limit):
    overall_score_field = f"{Sections.BRAND_SAFETY}.overall_score"
    video_source = (Sections.MAIN, overall_score_field, f"{Sections.GENERAL_DATA}.title", Sections.CUSTOM_PROPERTIES)
    query = (
        QueryBuilder().build().must().term().field("channel.id").value(channel_id).get()
        & QueryBuilder().build().must().exists().field(overall_score_field).get()
        & QueryBuilder().build().must().range().field(overall_score_field).lte(bs_score_limit).get()
    )
    videos_generator = Video.search().source(video_source).query(query).scan()
    yield from videos_generator
