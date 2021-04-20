import logging
from typing import Iterable

from bs4 import BeautifulSoup
from celery.exceptions import Retry
from datetime import timedelta
from django.conf import settings
from django.utils import timezone

from administration.notifications import send_email
from audit_tool.models import APIScriptTracker
from audit_tool.models import AuditVideoTranscript
from es_components.constants import Sections
from es_components.managers.transcript import TranscriptManager
from es_components.models.transcript import Transcript
from transcripts.constants import PROCESSOR_VERSION
from transcripts.constants import TranscriptSourceTypeEnum
from transcripts.constants import source_type_by_id
from transcripts.tasks.update_transcripts_from_cache import UPSERT_BACKOFF_EXCEPTIONS
from transcripts.utils import get_formatted_captions_from_soup
from utils.celery.tasks import lock
from utils.exception import backoff
from utils.utils import RunningAverage
from utils.utils import chunked_queryset


TRANSCRIPTS_UPDATE_ID_CEILING = 45831922


logger = logging.getLogger(__name__)


class Updater:

    EMAIL_LOCK_NAME = "migrate_es_transcripts_index_from_cache"
    CURSOR_KEY = "migrate_es_transcripts_index_from_cache_cursor"
    EMAIL_LIST = ["andrew.wong@channelfactory.com",]
    CHUNK_SIZE = 5
    counter_name_by_source_type = {
        TranscriptSourceTypeEnum.CUSTOM.value: "custom_transcripts_count",
        TranscriptSourceTypeEnum.WATSON.value: "watson_transcripts_count",
        TranscriptSourceTypeEnum.TTS_URL.value: "tts_url_transcripts_count"
    }

    def __init__(self):
        self.upsert_queue = []
        self.started_at = None
        self.cursor, _ = APIScriptTracker.objects.get_or_create(name=self.CURSOR_KEY, defaults={"cursor": 0})
        self.floor = 0
        self.ceiling = 0
        self.transcripts_to_process_count = 0
        self.videos_processed_count = 0
        self.chunks_processed_count = 0
        self.videos_skipped_count = 0
        self.latest_chunk_dur_seconds = 0
        self.latest_chunk_xscpt_count = 0
        self.skipped_no_text = 0
        self.skipped_no_source_type = 0
        self.avg_xscpt_per_chunk_count = RunningAverage()
        self.avg_chunk_dur_secs = RunningAverage()

        # count source types
        self.custom_transcripts_count = 0
        self.watson_transcripts_count = 0
        self.tts_url_transcripts_count = 0

    def run(self, floor: int = None, ceiling: int = TRANSCRIPTS_UPDATE_ID_CEILING):
        """
        main entrypoint into running the class, o
        :param floor:
        :param ceiling:
        :return:
        """
        # default floor to cursor if none is provided
        self.floor = self.cursor.cursor if floor is None else floor
        self.ceiling = ceiling
        if self.floor >= self.ceiling:
            self.floor = 0

        logger.info(f"starting from floor of {self.floor:,} to ceiling of {self.ceiling:,}")

        self.started_at = timezone.now()
        query = AuditVideoTranscript.objects.prefetch_related("video", "language") \
            .filter(id__gte=self.floor, id__lte=self.ceiling) \
            .order_by("id")
        self.transcripts_to_process_count = query.count()
        for chunk in chunked_queryset(queryset=query, chunk_size=self.CHUNK_SIZE):
            start = timezone.now()
            self._populate_upsert_queue(chunk)
            self._upsert()
            self._clear_upsert_queue()

            self.latest_chunk_dur_seconds = (timezone.now() - start).total_seconds()
            self.avg_chunk_dur_secs.update(self.latest_chunk_dur_seconds)
            self.avg_xscpt_per_chunk_count.update(self.latest_chunk_xscpt_count)
            self.chunks_processed_count += 1
            self.cursor.save(update_fields=["cursor"])
            self._report()

    def _report(self):
        """
        report progress, email every hour
        :return:
        """
        total_pct = round((self.cursor.cursor / self.ceiling) * 100, 2)
        transcripts_this_run = self.cursor.cursor - self.floor
        transcripts_this_run_pct = round((transcripts_this_run / self.ceiling) * 100, 2)
        chunks_remaining_count = round((self.ceiling - self.cursor.cursor) / self.CHUNK_SIZE)
        transcripts_skipped_count = sum([
            self.skipped_no_text,
            self.skipped_no_source_type
        ])
        transcripts_skipped_pct = round((transcripts_skipped_count / transcripts_this_run) * 100, 2)
        skipped_no_text_pct = round((self.skipped_no_text / transcripts_this_run) * 100, 2)
        skipped_no_source_pct = round((self.skipped_no_source_type / transcripts_this_run) * 100, 2)
        eta_seconds = round(self.avg_chunk_dur_secs.get(pretty=False) * chunks_remaining_count)
        message = (
            "\n"
            f"total transcripts progress: {total_pct}% (cursor: {self.cursor.cursor:,} ceiling: {self.ceiling:,}) \n"
            f"----- chunks processed this run: {self.chunks_processed_count:,} \n"
            f"----- latest chunk duration: {timedelta(seconds=self.latest_chunk_dur_seconds)} \n"
            f"----- average chunk duration: {timedelta(seconds=self.avg_chunk_dur_secs.get(pretty=False))} \n"
            f"----- transcripts this chunk: {self.latest_chunk_xscpt_count:,} \n"
            f"----- average transcripts per chunk: {round(self.avg_xscpt_per_chunk_count.get(pretty=False), 2):,} \n"
            f"----- chunks to completion: {chunks_remaining_count:,} \n"
            f"----- runtime: {timezone.now() - self.started_at} \n"
            f"----- estimated time to completion: {timedelta(seconds=eta_seconds)} \n"
            f"total transcripts remaining: {self.transcripts_to_process_count - self.cursor.cursor:,} \n"
            f"transcripts processed this run: {transcripts_this_run:,} ({transcripts_this_run_pct}%) \n"
            f"transcripts skipped this run: {transcripts_skipped_count:,} ({transcripts_skipped_pct}%) \n"
            f"----- no text: {self.skipped_no_text:,} ({skipped_no_text_pct}%) \n"
            f"----- no source type: {self.skipped_no_source_type:,} ({skipped_no_source_pct}%) \n"
        )
        logger.info(message)

        try:
            lock(lock_name=self.EMAIL_LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Retry:
            return

        subject = f"Migrate ES transcripts from cache progress: ({total_pct}%)"
        send_email(
            subject=subject,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=self.EMAIL_LIST,
            message=message
        )
        logger.info("progress email sent!")


    def _clear_upsert_queue(self):
        """
        empties the upsert queue for another chunk
        :return:
        """
        self.upsert_queue = []

    @backoff(max_backoff=600, exceptions=UPSERT_BACKOFF_EXCEPTIONS)
    def _upsert(self):
        """
        upsert items in the upsert queue
        :return:
        """
        manager = TranscriptManager(upsert_sections=(Sections.TEXT, Sections.VIDEO, Sections.GENERAL_DATA))
        manager.upsert(self.upsert_queue)
        self.latest_chunk_xscpt_count = len(self.upsert_queue)

    def _populate_upsert_queue(self, chunk: Iterable):
        """
        handle a chunk of AuditVideoTranscripts
        :param chunk:
        :return:
        """
        for pg_transcript in chunk:
            self.cursor.cursor = pg_transcript.id

            soup = BeautifulSoup(pg_transcript.transcript, "xml")
            transcript_text = get_formatted_captions_from_soup(soup)
            if not transcript_text:
                self.skipped_no_text += 1
                continue

            source_type = source_type_by_id.get(pg_transcript.source)
            if not source_type:
                self.skipped_no_source_type += 1
                continue

            video_id = pg_transcript.video.video_id
            es_transcript = Transcript(pg_transcript.id)
            es_transcript.populate_video(id=video_id)
            es_transcript.populate_text(value=transcript_text)
            es_transcript.populate_general_data(
                language_code=pg_transcript.language.language,
                source_type=source_type,
                is_asr=source_type == TranscriptSourceTypeEnum.TTS_URL.value,
                processor_version=PROCESSOR_VERSION,
                processed_at=timezone.now())
            self.upsert_queue.append(es_transcript)

    def _increment_source_type_counts(self, source_type: str):
        """
        increment source type count by a given source type string
        :param source_type:
        :return:
        """
        counter_name = self.counter_name_by_source_type.get(source_type)
        if not source_type:
            return
        counter = getattr(self, counter_name, None)
        if not counter:
            return
        counter += 1
