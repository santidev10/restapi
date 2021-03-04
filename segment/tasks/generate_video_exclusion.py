import concurrent.futures
import csv
import logging
import os
import tempfile
from typing import List
import operator

from django.conf import settings
from uuid import uuid4

from audit_tool.models import get_hash_name
from es_components.constants import Sections
from es_components.models import Video
from es_components.constants import SortDirections
from es_components.query_builder import QueryBuilder
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models.constants import SegmentTypeEnum
from utils.lang import merge
from utils.utils import chunks_generator
from utils.datetime import now_in_default_tz
from utils.brand_safety import map_score_threshold


logger = logging.getLogger(__name__)
LIMIT = 125000


def generate_video_exclusion(channel_ctl: CustomSegment, channel_ids):
    """
    Generate video exclusion list using channels from Channel CTL
    The video exclusion list is generated from the videos of the channels in channel_ctl. If a channel's video
    is blocklisted, it is prioritized to go on the final list.
    If there is remaining space from LIMIT, then the list is supplemented by videos that are either YouTube age
    restricted or have a programmatic brand safety score < the score threshold of the channel_ctl
        e.g. channel_ctl was created with Suitable filter, all videos should have a score of less than Suitable
    Lower brand safety scores have priority of being on the list over higher brand safety scores
    :param channel_ctl: Channel CTL that will be used as source channels to retrieve videos
    :param channel_ids:
    :return:
    """
    all_blocklist = []
    all_videos = []
    video_exclusion_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        score_threshold = map_score_threshold(channel_ctl.export.query["params"]["score_threshold"])
        for chunk in chunks_generator(channel_ids, size=5):
            curr_blocklist = []
            curr_videos = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(get_videos_for_channel, channel_id, score_threshold) for channel_id in chunk]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    _sort_videos(result, curr_blocklist, curr_videos)
            # Sort videos that are not blocklisted to merge with all_videos
            curr_videos.sort(key=lambda doc: doc.brand_safety.overall_score)
            all_videos = merge(all_videos, curr_videos, lambda doc: doc.brand_safety.overall_score)
            all_blocklist.extend(curr_blocklist)

            all_videos = all_videos[:LIMIT]
            # If blocklist videos exceeds limit, then list should only consist of blocklist videos
            if len(all_blocklist) >= LIMIT:
                break

        all_results = (all_blocklist + all_videos)[:LIMIT]
        rows = []
        for video in all_results:
            row = [f"https://www.youtube.com/watch?v={video.main.id}", video.general_data.title]
            # serialize blocklist overall score as -1
            overall_score = video.custom_properties.blocklist if video.custom_properties.blocklist is True \
                else video.brand_safety.overall_score
            row.append(overall_score)
            rows.append(row)

        with open(video_exclusion_fp, mode="w+") as file:
            writer = csv.writer(file)
            writer.writerow(["URL", "title", "score"])
            writer.writerows(rows)
        video_exclusion_s3_key = f"{uuid4()}.csv"
        channel_ctl.s3.export_file_to_s3(video_exclusion_fp, video_exclusion_s3_key)
    except Exception:
        logger.exception(f"Uncaught exception for generate_videos_exclusion({channel_ctl, channel_ids})")
    else:
        title = f"{channel_ctl.title}_video_exclusion_list"
        video_exclusion_ctl, _ = CustomSegment.objects.update_or_create(
            segment_type=SegmentTypeEnum.VIDEO.value,
            owner_id=channel_ctl.owner_id,
            title=title,
            title_hash=get_hash_name(title),
            defaults=dict(
                statistics={
                    "channel_ctl_source_id": channel_ctl.id
                }
            )
        )
        CustomSegmentFileUpload.objects.update_or_create(
            segment=video_exclusion_ctl,
            defaults=dict(
                completed_at=now_in_default_tz(),
                filename=video_exclusion_s3_key,
                query={},
            )
        )
        return video_exclusion_ctl
    finally:
        os.remove(video_exclusion_fp)


def get_videos_for_channel(channel_id, bs_score_limit):
    overall_score_field = f"{Sections.BRAND_SAFETY}.overall_score"
    video_source = (Sections.MAIN, overall_score_field, f"{Sections.GENERAL_DATA}.title", Sections.CUSTOM_PROPERTIES)
    query = (
        QueryBuilder().build().must().term().field("channel.id").value(channel_id).get()
        & QueryBuilder().build().must().exists().field(overall_score_field).get()
        & QueryBuilder().build().must().range().field(overall_score_field).lte(bs_score_limit).get()
    )
    videos_generator = Video.search().sort({overall_score_field: {"order": SortDirections.ASCENDING}},).source(video_source).query(query).scan()
    yield from videos_generator


def _sort_videos(videos, blocklist_list, videos_list):
    for video in videos:
        if video.custom_properties.blocklist is True:
            container = blocklist_list
        else:
            container = videos_list
        container.append(video)


def _channels_generator(fp):
    with open(fp, mode="r") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            yield row[0]

