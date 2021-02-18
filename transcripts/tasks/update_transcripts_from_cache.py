import logging
from typing import Iterable

from bs4 import BeautifulSoup
from celery.exceptions import Retry
from datetime import timedelta
from django.conf import settings
from django.utils import timezone
from elasticsearch.helpers.errors import BulkIndexError
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from urllib3.exceptions import ProtocolError
from http.client import IncompleteRead

from administration.notifications import send_email
from audit_tool.models import AuditVideoTranscript
from transcripts.tasks.update_tts_url_transcripts import TRANSCRIPTS_UPDATE_ID_CEILING
from transcripts.utils import get_formatted_captions_from_soup
from utils.celery.tasks import lock
from utils.exception import backoff
from utils.transform import populate_video_custom_captions
from utils.utils import chunked_queryset


logger = logging.getLogger(__name__)


class TranscriptsFromCacheUpdater:

    CHUNK_SIZE = 5000
    LOCK_NAME = "update_transcripts_from_cache"
    EMAIL_LIST = ["andrew.wong@channelfactory.com"]

    def __init__(self):
        self.cursor = 0
        self.videos_map = {}
        self.upsert_queue = []
        self.skipped_count = 0
        self.not_xml_count = 0
        self.processed_count = 0
        self.no_es_record_count = 0
        self.no_es_transcript_count = 0
        self.total_to_process_count = 0
        self.no_cached_transcript_count = 0
        self.manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY, Sections.TASK_US_DATA),
                                    upsert_sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY))

    def run(self, floor: int = 0, ceiling: int = TRANSCRIPTS_UPDATE_ID_CEILING):
        """
        main interface for running update script. optionally specify an inclusive floor from which to start, and an
        inclusive ceiling at which to end
        :param floor:
        :param ceiling:
        :return:
        """
        query = AuditVideoTranscript.objects.prefetch_related("video").filter(id__gte=floor, id__lte=ceiling) \
            .order_by("id")
        self.total_to_process_count = query.count()
        for chunk in chunked_queryset(query, self.CHUNK_SIZE):
            self._handle_videos_chunk(chunk)

    def _handle_videos_chunk(self, chunk: Iterable):
        """
        handle a chunk of videos
        :param chunk:
        :return:
        """
        self._map_es_videos(chunk)
        for video in chunk:
            self._handle_video(video)
        try:
            self._upsert_chunk()
        except (IncompleteRead, ProtocolError) as e:
            logger.info(f"caught exception of type: {type(e).__name__}. {e}")
            pass
        self.upsert_queue = []
        self._report()

    def _report(self):
        """
        reports current progress
        :return:
        """
        percentage = round((self.processed_count / self.total_to_process_count) * 100, 2)
        skipped_percentage = round((self.skipped_count / self.total_to_process_count) * 100, 2)
        not_xml_percentage = round((self.not_xml_count / self.total_to_process_count) * 100, 2)
        no_es_record_percentage = round((self.no_es_record_count / self.total_to_process_count) * 100, 2)
        no_es_transcript_percentage = round((self.no_es_transcript_count / self.total_to_process_count) * 100, 2)
        no_cached_transcript_percentage = round((self.no_cached_transcript_count / self.total_to_process_count) * 100,
                                                2)

        message = (
            f"cursor: {self.cursor} \n"
            f"processed {self.processed_count} of {self.total_to_process_count} ({percentage}%) \n"
            f"total skipped: {self.skipped_count} ({skipped_percentage}%) \n"
            f"----- not xml: {self.not_xml_count} ({not_xml_percentage}%) \n"
            f"----- no es record: {self.no_es_record_count} ({no_es_record_percentage}%) \n"
            f"----- no es transcript: {self.no_es_transcript_count} ({no_es_transcript_percentage}%) \n"
            f"----- no cached transcript: {self.no_cached_transcript_count} ({no_cached_transcript_percentage}%) \n"
        )
        logger.info(message)

        try:
            lock(lock_name=self.LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Retry:
            return

        subject = f"Update transcripts from cache progress: ({percentage}%)"
        send_email(
            subject=subject,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=self.EMAIL_LIST,
            message=message
        )

    def _map_es_videos(self, chunk: Iterable):
        """
        create a id-keyed map for the current chunk
        :param chunk:
        :return:
        """
        video_ids = [video.video.video_id for video in chunk]
        es_videos = self.manager.get(video_ids)
        self.videos_map = {video.main.id: video for video in es_videos
                           if hasattr(video, "main") and hasattr(video.main, "id")}

    def _handle_video(self, video: AuditVideoTranscript):
        """
        handle the processing of a single video item
        :param video:
        :return:
        """
        self.cursor = video.id
        video_id = video.video.video_id

        if video.transcript is None:
            self.skipped_count += 1
            self.no_cached_transcript_count += 1
            return

        if "xml" not in video.transcript:
            self.skipped_count += 1
            self.not_xml_count += 1
            return

        es_video = self.videos_map.get(video_id, None)
        if es_video is None:
            self.skipped_count += 1
            self.no_es_record_count += 1
            return

        soup = BeautifulSoup(video.transcript, "xml")
        captions = get_formatted_captions_from_soup(soup)
        language = self._get_language_from_video(es_video)
        if language is None:
            self.skipped_count += 1
            self.no_es_transcript_count += 1
            return
        populate_video_custom_captions(es_video, transcript_texts=[captions], transcript_languages=[language],
                                       source="tts_url", asr_lang=language)
        self.upsert_queue.append(es_video)
        self.processed_count += 1

    # exp. backoff w/ noise, intended to catch ES query queue limit exceeded exception
    @backoff(max_backoff=120, exceptions=(BulkIndexError,))
    def _upsert_chunk(self):
        """
        upsert the current upsert queue
        :return:
        """
        self.manager.upsert(self.upsert_queue)

    def _get_language_from_video(self, video: Video):
        """
        get the language from the last video transcript item.
        :param video:
        :return:
        """
        try:
            item = video.custom_captions.items[-1]
        except IndexError:
            return None
        return item.language_code
