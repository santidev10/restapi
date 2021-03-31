import concurrent.futures
import csv
import logging
import os
import tempfile
from typing import List

from botocore.exceptions import ClientError
from django.conf import settings
from uuid import uuid4

from es_components.managers import VideoManager
from es_components.constants import Sections
from es_components.models import Video
from es_components.query_builder import QueryBuilder
from saas import celery_app
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.constants import Results
from utils.lang import merge
from utils.exception import retry
from utils.utils import chunks_generator
from utils.brand_safety import map_score_threshold
from utils.search_after import search_after


logger = logging.getLogger(__name__)
LIMIT = 125000


@celery_app.task
def generate_video_exclusion(channel_ctl_id: int) -> str:
    """
    Wrapper for celery task decorator
    :param channel_ctl_id: int
    :return:
    """
    video_exclusion_s3_key = _generate_video_exclusion(channel_ctl_id)
    return video_exclusion_s3_key


@retry(count=5, delay=10)
def _generate_video_exclusion(channel_ctl_id: int):
    """
    Generate video exclusion list using channels from Channel CTL
    The video exclusion list is generated from the videos of the channels in channel_ctl. If a channel's video
    is blocklisted, it is prioritized to go on the final list.
    If there is remaining space from LIMIT, then the list is supplemented by videos that have a programmatic
        brand safety score <= the score threshold of the channel_ctl. Lowest scores are prioritized
        e.g. channel_ctl was created with Suitable filter, all videos should have a score of less than Suitable
    Lower brand safety scores have priority of being on the list over higher brand safety scores

    Lastly saves video exclusion filename on channel_ctl statistics dict
    :param channel_ctl_id: Channel CTL that will be used as source channels to retrieve videos
    :return:
    """
    all_blocklist = []
    all_videos = []
    video_manager = VideoManager(sections=[Sections.BRAND_SAFETY, Sections.GENERAL_DATA, Sections.CUSTOM_PROPERTIES])
    channel_ctl = CustomSegment.objects.get(id=channel_ctl_id)
    try:
        channel_ids = channel_ctl.s3.get_extract_export_ids()
    except Exception:
        logger.exception(
            f"Uncaught exception for generate_videos_exclusion in "
            f"get_extract_export_ids: {channel_ctl.title}: {channel_ctl.id}", exc_info=True)
        channel_ctl.statistics.update({
            Results.VIDEO_EXCLUSION_FILENAME: False,
        })
        channel_ctl.params.update({
            Params.VideoExclusion.WITH_VIDEO_EXCLUSION: False
        })
        channel_ctl.save(update_fields=["params", "statistics"])
        return

    video_exclusion_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        mapped_score_threshold = map_score_threshold(channel_ctl.params[Params.VideoExclusion.VIDEO_EXCLUSION_SCORE_THRESHOLD])
        for chunk in chunks_generator(channel_ids, size=200):
            curr_blocklist = []
            curr_videos = []
            chunk = list(chunk)
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(get_videos_for_channels, list(ids), mapped_score_threshold, video_manager)
                           for ids in chunks_generator(chunk, size=10)]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    _separate_videos(result, curr_blocklist, curr_videos)
            # Sort videos that are not blocklisted to merge with all_videos
            curr_videos.sort(key=lambda doc: doc.brand_safety.overall_score)
            all_videos = merge(all_videos, curr_videos, lambda doc: doc.brand_safety.overall_score)
            all_blocklist.extend(curr_blocklist)
            all_videos = all_videos[:LIMIT]
            # If blocklist videos exceeds limit, then list should only consist of blocklist videos
            if len(all_blocklist) >= LIMIT:
                break

        all_results = (all_blocklist + all_videos)[:LIMIT]
        video_exclusion_s3_key = _export_results(channel_ctl, video_exclusion_fp, all_results)
    except Exception:
        partial_results = (all_blocklist + all_videos)[:LIMIT]
        _save_partial_results(channel_ctl, partial_results, video_exclusion_fp)
        logger.exception(f"Uncaught exception for generate_videos_exclusion({channel_ctl.title}: {channel_ctl.id})")
        # Raise for retry decorator
        raise
    else:
        channel_ctl.statistics[Results.VIDEO_EXCLUSION_FILENAME] = video_exclusion_s3_key
        channel_ctl.save(update_fields=["statistics"])
        return video_exclusion_s3_key


@retry(20, 2)
def get_videos_for_channels(channel_ids: list, bs_score_limit: int, video_manager) -> iter:
    """
    Retrieve videos using channel_ids
    :param channel_ids: Channel ids to retrieve videos for
    :param bs_score_limit: Filter brand_safety.overall_score using bs_score_limit
        bs_score_limit is the original score_threshold the channel ctl was created for, and the resulting video
        brand safety scores should be <= the bs_score_limit
    :return:
    """
    results = []
    overall_score_field = f"{Sections.BRAND_SAFETY}.overall_score"
    video_source = (
    Sections.MAIN, overall_score_field, f"{Sections.GENERAL_DATA}.title", f"{Sections.CUSTOM_PROPERTIES}.blocklist")
    query = (
        QueryBuilder().build().must().terms().field("channel.id").value(channel_ids).get()
        & QueryBuilder().build().must().exists().field(overall_score_field).get()
        & QueryBuilder().build().must().range().field(overall_score_field).lt(bs_score_limit).get()
        & QueryBuilder().build().must_not().exists().field(Sections.DELETED).get()
    )
    for batch in search_after(query, video_manager, source=video_source, size=500):
        results.extend(batch)
    return results


def _separate_videos(videos: iter, blocklist_list: list, videos_list: list) -> None:
    """
    Separate videos into either blocklist or videos list
    blocklisted videos are prioritized on final list but nonblocklisted videos must be sorted with all videos
    :param videos: List[list] -> Generator result from bulk_search. Each videos yield is a list itself
    :param blocklist_list: List object to contain current batch of blocklist videos
    :param videos_list: List object to contain current batch of non blocklisted videos
    :return: None
    """
    for video in videos:
        if video.custom_properties.blocklist is True:
            container = blocklist_list
        else:
            container = videos_list
        container.append(video)


def _export_results(channel_ctl: CustomSegment, export_fp: str, results: List[Video]) -> str:
    """
    Write results to file and export to S3
    :param s3: S3Exporter
    :param export_fp: Filepath to write results
    :param results: Video exclusion results
    :return: str
    """
    rows = [
        [f"https://www.youtube.com/watch?v={video.main.id}", video.general_data.title]
        for video in results
    ]
    with open(export_fp, mode="w+") as file:
        writer = csv.writer(file)
        writer.writerow(["URL", "Title"])
        writer.writerows(rows)
    video_exclusion_s3_key = f"{uuid4()}.csv"
    s3 = channel_ctl.s3
    content_disposition = s3.get_content_disposition(f"{channel_ctl.title}_video_exclusion.csv")
    s3.export_file_to_s3(export_fp, video_exclusion_s3_key,
                         extra_args=dict(ContentDisposition=content_disposition))
    return video_exclusion_s3_key


def _save_partial_results(channel_ctl: CustomSegment, partial_results: list, video_exclusion_fp: str):
    """
    Save partial results as generation task is retried
    Will save new filename if partial results file is larger than previous file
    :param channel_ctl: CustomSegment being processed
    :param partial_results: Results at the time of exception being raised
    :return:
    """
    try:
        prev_file = channel_ctl.statistics.get(Results.VIDEO_EXCLUSION_FILENAME)
        video_exclusion_s3_key = _export_results(channel_ctl, video_exclusion_fp, partial_results)
        if not prev_file or channel_ctl.s3.check_key_size(prev_file) < channel_ctl.s3.check_key_size(video_exclusion_s3_key):
            channel_ctl.statistics[Results.VIDEO_EXCLUSION_FILENAME] = video_exclusion_s3_key
            channel_ctl.save(update_fields=["statistics"])
    except ClientError:
        pass
    try:
        os.remove(video_exclusion_fp)
    except OSError:
        pass
