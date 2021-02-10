from typing import Iterable

from bs4 import BeautifulSoup
from elasticsearch.helpers.errors import BulkIndexError
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video

from audit_tool.models import AuditVideoTranscript
from transcripts.tasks.update_tts_url_transcripts import TRANSCRIPTS_UPDATE_ID_CEILING
from transcripts.utils import get_formatted_captions_from_soup
from utils.exception import backoff
from utils.transform import populate_video_custom_captions
from utils.utils import chunked_queryset


class TranscriptsFromCacheUpdater:
    CHUNK_SIZE = 5000

    def __init__(self):
        self.videos_map = {}
        self.upsert_queue = []
        self.skipped_count = 0
        self.not_xml_count = 0
        self.no_es_record_count = 0
        self.no_es_transcript_count = 0
        self.no_cached_transcript_count = 0
        self.manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY, Sections.TASK_US_DATA),
                                    upsert_sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY))

    def run(self):
        query = AuditVideoTranscript.objects.prefetch_related("video").filter(id__lte=TRANSCRIPTS_UPDATE_ID_CEILING) \
            .order_by("id")
        for chunk in chunked_queryset(query, self.CHUNK_SIZE):
            self._handle_videos_chunk(chunk)

    def _handle_videos_chunk(self, chunk: Iterable):
        """
        handle a chunk of videos
        :param chunk:
        :return:
        """
        video_transcript_ids = [item.id for item in chunk]
        first_id = video_transcript_ids[0]
        self._report(int(first_id))
        self._map_es_videos(chunk)
        for video in chunk:
            self._handle_video(video)
        self._upsert_chunk()
        self.upsert_queue = []

    def _report(self, current_id: int):
        """
        reports current progress
        :param current_id:
        :return:
        """
        percentage = round((int(current_id) / TRANSCRIPTS_UPDATE_ID_CEILING) * 100, 2)
        print(f"processing {current_id} of {TRANSCRIPTS_UPDATE_ID_CEILING} ({percentage}%)")
        skipped_percentage = round((self.skipped_count / TRANSCRIPTS_UPDATE_ID_CEILING) * 100, 2)
        print(f"total skipped: {self.skipped_count} ({skipped_percentage}%)")
        not_xml_percentage = round((self.not_xml_count / TRANSCRIPTS_UPDATE_ID_CEILING) * 100, 2)
        print(f"----- not xml: {self.not_xml_count} ({not_xml_percentage}%)")
        no_es_record_percentage = round((self.no_es_record_count / TRANSCRIPTS_UPDATE_ID_CEILING) * 100, 2)
        print(f"----- no es record: {self.no_es_record_count} ({no_es_record_percentage}%)")
        no_es_transcript_percentage = round((self.no_es_transcript_count / TRANSCRIPTS_UPDATE_ID_CEILING) * 100, 2)
        print(f"----- no es transcript: {self.no_es_transcript_count} ({no_es_transcript_percentage}%)")
        no_cached_transcript_percentage = round((self.no_cached_transcript_count / TRANSCRIPTS_UPDATE_ID_CEILING) * 100,
                                                2)
        print(f"----- no es transcript: {self.no_es_transcript_count} ({no_cached_transcript_percentage}%)")

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
