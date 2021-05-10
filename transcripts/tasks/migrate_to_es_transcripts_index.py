import logging
from time import sleep
from typing import Iterable
from typing import Union

from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from celery.exceptions import Retry
from datetime import timedelta
from django.conf import settings
from django.utils import timezone
from elasticapm.transport.exceptions import TransportException
from elasticsearch_dsl import Q
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers.errors import BulkIndexError
from urllib3.exceptions import ReadTimeoutError

from administration.notifications import send_email
from audit_tool.models import APIScriptTracker
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditVideoTranscript
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from es_components.query_builder import QueryBuilder
from es_components.constants import Sections
from es_components.managers.transcript import TranscriptManager
from es_components.managers.video import VideoManager
from es_components.models.transcript import Transcript
from transcripts.constants import AuditVideoTranscriptSourceTypeIdEnum
from transcripts.constants import PROCESSOR_VERSION
from transcripts.constants import TranscriptSourceTypeEnum
from transcripts.constants import source_type_by_id
from transcripts.utils import get_formatted_captions_from_soup
from utils.celery.tasks import lock
from utils.exception import backoff
from utils.utils import RunningAverage
from utils.utils import chunked_queryset


logger = logging.getLogger(__name__)


UPSERT_BACKOFF_EXCEPTIONS = (TransportError, TransportException, ReadTimeoutError, BulkIndexError,)
CACHED_CEILING_NAME = "migrate_to_es_transcripts_index_ceiling"
CACHED_CURSOR_NAME = "migrate_to_es_transcripts_index_cursor"
# limit update to only Videos with view count over 10M on RC. No limit for Prod
VIDEO_VIEWS_THRESHOLD = settings.TRANSCRIPTS_MIGRATION_VIDEO_VIEWS_THRESHOLD


class Task:

    CHUNK_SIZE = 5000
    SLEEP_SECONDS = 0
    EMAIL_LOCK_NAME = "migrate_to_es_transcripts_index_email"
    EMAIL_LIST = ["andrew.wong@channelfactory.com", "alex.peace@channelfactory.com"]

    def __init__(self):
        self.cursor = 0
        self.es_video_by_video_id = {}
        self.pg_transcripts_by_video_id = {}
        self.video_upsert_queue = []
        self.transcripts_upsert_queue = []
        self.floor = 0
        self.ceiling = 0
        self.counts_by_lang_code = {}
        self.videos_skipped_count = 0
        self.videos_processed_count = 0
        self.no_es_record_count = 0
        self.no_pg_transcripts_count = 0
        self.transcripts_to_process_count = 0
        self.empty_pg_transcripts_count = 0
        self.custom_transcripts_count = 0
        self.watson_transcripts_count = 0
        self.tts_url_transcripts_count = 0
        self.source_type_by_id = source_type_by_id.copy()
        # chunking
        self.chunks_count = 0
        self.latest_chunk_dur_seconds = 0
        self.avg_chunk_dur_secs = RunningAverage()
        self.latest_chunk_video_count = 0
        self.avg_vids_per_chunk_count = RunningAverage()
        self.latest_chunk_transcripts_ct = 0
        self.avg_transcripts_per_chunk_count = RunningAverage()
        self.en_language = None
        self.started_at = None
        self.video_manager = VideoManager(upsert_sections=(Sections.CUSTOM_CAPTIONS,))
        self.transcript_manager = TranscriptManager(upsert_sections=(Sections.TEXT, Sections.VIDEO,
                                                                     Sections.GENERAL_DATA))

    def run(self, floor: int = None, ceiling: int = None):
        """
        main interface for running update script. optionally specify an inclusive floor from which to start, and an
        inclusive ceiling at which to end
        :param floor:
        :param ceiling:
        :return:
        """
        self.started_at = timezone.now()
        self._set_bounds(floor=floor, ceiling=ceiling)

        query = AuditVideoTranscript.objects.prefetch_related("video", "language") \
            .filter(id__gte=self.floor, id__lte=self.ceiling) \
            .order_by("id")
        self.transcripts_to_process_count = query.count()
        for chunk in chunked_queryset(query, self.CHUNK_SIZE):
            start = timezone.now()
            self._handle_transcripts_chunk(chunk)
            # persist the cursor
            APIScriptTracker.objects.filter(name=CACHED_CURSOR_NAME).update(cursor=self.cursor)
            self.latest_chunk_dur_seconds = (timezone.now() - start).total_seconds()
            self.chunks_count += 1
            self.avg_chunk_dur_secs.update(self.latest_chunk_dur_seconds)
            self.avg_vids_per_chunk_count.update(self.latest_chunk_video_count)
            self.avg_transcripts_per_chunk_count.update(self.latest_chunk_transcripts_ct)
            try:
                self._report()
            except ClientError:
                pass  # issue with SES emailer. proceed

            if self.SLEEP_SECONDS:
                logger.info(f"sleeping for {self.SLEEP_SECONDS}")
                sleep(self.SLEEP_SECONDS)

        self._clean_up()

    def _set_bounds(self, floor: Union[int, None], ceiling: Union[int, None]) -> None:
        """
        set the pg transcripts run floor and ceiling. use specified values if passed, fall back to cached values if
        present, else fall back to lowest floor, highest ceiling
        :param floor:
        :param ceiling:
        :return:
        """
        # set floor
        floor = floor if isinstance(floor, int) else 0
        cached_floor, created = APIScriptTracker.objects.get_or_create(name=CACHED_CURSOR_NAME,
                                                                       defaults={"cursor": floor})
        floor = cached_floor.cursor
        message = f"floor not cached, running from: {floor}" \
            if created else f"floor cached, running from cached cursor: {floor}"
        logger.info(message)
        self.floor = floor

        # set ceiling
        ceiling = ceiling if isinstance(ceiling, int) else AuditVideoTranscript.objects.order_by("-id").first().id
        cached_ceiling, created = APIScriptTracker.objects.get_or_create(name=CACHED_CEILING_NAME,
                                                                         defaults={"cursor": ceiling})
        ceiling = cached_ceiling.cursor
        message = f"ceiling not cached, running to highest AuditVideoTranscript id: {ceiling}" \
            if created else f"ceiling cached, running to ceiling: {ceiling}"
        logger.info(message)
        self.ceiling = ceiling

    def _clean_up(self):
        """
        clean up after task is complete
        :return:
        """
        message = (f"finished migrate_to_es_transcripts_index task, deleting cached cursor ({self.cursor:,}) and \n"
                   f"ceiling ({self.ceiling:,})")
        logger.info(message)
        APIScriptTracker.objects.filter(name__in=[CACHED_CEILING_NAME, CACHED_CURSOR_NAME]).delete()
        send_email(
            subject=f"migrate to ES transcripts index task complete!",
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=self.EMAIL_LIST,
            message=message
        )

    def _handle_transcripts_chunk(self, chunk: Iterable):
        """
        handle a chunk of pg transcripts
        :param chunk:
        :return:
        """
        self._map_es_videos(chunk)

        # we need to map transcripts to video id to handle multiple languages/transcripts per video
        self._map_pg_transcripts(chunk)

        video_ids = self.pg_transcripts_by_video_id.keys()
        for video_id in video_ids:
            self._handle_video(video_id)

        self._upsert_videos()
        self._upsert_transcripts()
        self.video_upsert_queue = []
        self.transcripts_upsert_queue = []

    def _report(self):
        """
        reports current progress
        :return:
        """
        total_pct = round((self.cursor / self.ceiling) * 100, 2)
        transcripts_this_run = self.cursor - self.floor
        transcripts_this_run_pct = round((transcripts_this_run / self.ceiling) * 100, 2)
        transcripts_processed_count = sum([
            self.custom_transcripts_count,
            self.watson_transcripts_count,
            self.tts_url_transcripts_count
        ])
        custom_pct = round((self.custom_transcripts_count / transcripts_processed_count) * 100, 2)
        tts_url_pct = round((self.tts_url_transcripts_count / transcripts_processed_count) * 100, 2)
        watson_pct = round((self.watson_transcripts_count / transcripts_processed_count) * 100, 2)

        skipped_pct = round((self.videos_skipped_count / self.transcripts_to_process_count) * 100, 2)
        no_es_record_pct = round((self.no_es_record_count / self.transcripts_to_process_count) * 100, 2)
        no_pg_transcripts_pct = round((self.no_pg_transcripts_count / self.transcripts_to_process_count) * 100, 2)
        empty_pg_transcripts_pct = round((self.empty_pg_transcripts_count / self.transcripts_to_process_count) * 100,
                                         2)

        chunks_remaining_count = round((self.ceiling - self.cursor) / self.CHUNK_SIZE)
        eta_seconds = round(self.avg_chunk_dur_secs.get(pretty=False) * chunks_remaining_count)
        message = (
            "\n"
            f"total transcripts progress: {total_pct}% (cursor: {self.cursor:,} ceiling: {self.ceiling:,}) \n"
            f"----- video views threshold: {VIDEO_VIEWS_THRESHOLD:,} \n"
            f"----- chunks processed this run: {self.chunks_count:,} \n"
            f"----- latest chunk duration: {timedelta(seconds=self.latest_chunk_dur_seconds)} \n"
            f"----- average chunk duration: {timedelta(seconds=self.avg_chunk_dur_secs.get(pretty=False))} \n"
            f"----- videos this chunk: {self.latest_chunk_video_count:,} \n"
            f"----- average videos per chunk: {round(self.avg_vids_per_chunk_count.get(pretty=False), 2):,} \n"
            f"----- transcripts this chunk: {self.latest_chunk_transcripts_ct:,} \n"
            f"----- average transcripts per chunk: {round(self.avg_transcripts_per_chunk_count.get(pretty=False), 2):,} \n"
            f"----- chunks to completion: {chunks_remaining_count:,} \n"
            f"----- runtime: {timezone.now() - self.started_at} \n"
            f"----- estimated time to completion: {timedelta(seconds=eta_seconds)} \n"
            f"videos processed this run: {self.videos_processed_count:,} \n"
            f"videos skipped this run: {self.videos_skipped_count:,} ({skipped_pct}%) \n"
            f"----- no es record: {self.no_es_record_count:,} ({no_es_record_pct}%) \n"
            f"----- no PG transcripts: {self.no_pg_transcripts_count:,} ({no_pg_transcripts_pct}%) \n"
            f"----- empty PG transcripts: {self.empty_pg_transcripts_count:,} ({empty_pg_transcripts_pct}%) \n"
            f"total transcripts remaining: {self.transcripts_to_process_count - self.cursor:,} \n"
            f"transcripts processed this run: {transcripts_this_run:,} ({transcripts_this_run_pct})% \n"
            f"----- custom transcripts: {self.custom_transcripts_count:,} ({custom_pct}% of processed) \n"
            f"----- tts_url transcripts: {self.tts_url_transcripts_count:,} ({tts_url_pct}% of processed) \n"
            f"----- watson transcripts: {self.watson_transcripts_count:,} ({watson_pct}% of processed) \n"
        )
        logger.info(message)

        try:
            lock(lock_name=self.EMAIL_LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Retry:
            return

        subject = f"Migrate to ES transcripts index progress: ({total_pct}%)"
        send_email(
            subject=subject,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=self.EMAIL_LIST,
            message=message
        )
        logger.info("progress email sent!")

    def _map_pg_transcripts(self, chunk: Iterable):
        """
        create an id > list map, to store a list of all PG AuditVideoTranscripts per video id
        :param chunk:
        :return:
        """
        self.pg_transcripts_by_video_id = {}
        for transcript in chunk:
            video_id = transcript.video.video_id
            transcripts = self.pg_transcripts_by_video_id.get(video_id, [])
            transcripts.append(transcript)
            self.pg_transcripts_by_video_id[video_id] = transcripts

        # we need to get all priority transcripts for a video, in order to figure out top five transcripts by language
        video_ids = []
        for video_id, transcripts in self.pg_transcripts_by_video_id.items():
            if len(transcripts) > 1:
                video_ids.append(video_id)
        # we're done here if no additional transcripts to process
        if not len(video_ids):
            return

        # add transcripts to the map
        transcripts = self._get_all_transcripts_by_video_ids(video_ids=video_ids)
        for transcript in transcripts:
            video_id = transcript.video.video_id
            transcript_items = self.pg_transcripts_by_video_id.get(video_id, [])
            if transcript in transcript_items:
                continue
            transcript_items.append(transcript)
            self.pg_transcripts_by_video_id[video_id] = transcript_items

    @staticmethod
    def _get_all_transcripts_by_video_ids(video_ids: list):
        """
        get all transcripts for the specified video ids
        :param video_ids:
        :return:
        """
        return AuditVideoTranscript.objects.filter(video__video_id__in=video_ids).prefetch_related("video", "language")

    def _map_es_videos(self, chunk: Iterable):
        """
        create a id-keyed map for the current chunk
        :param chunk:
        :return:
        """
        video_ids = list(set([video.video.video_id for video in chunk]))
        logger.info(f"requesting {len(video_ids):,} Videos from ES")
        # only get videos that aren't deleted, and optionally, are above a certain view threshold
        query = Q("bool")
        query &= QueryBuilder().build().must().terms().field(f"{Sections.MAIN}.id").value(video_ids).get()
        query &= QueryBuilder().build().must_not().exists().field(f"{Sections.DELETED}").get()
        if VIDEO_VIEWS_THRESHOLD:
            query &= QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(VIDEO_VIEWS_THRESHOLD)\
                .get()
        es_videos = self.video_manager.search(query=query)
        logger.info(f"got {es_videos.count():,} Videos from ES after constraints")
        self.es_video_by_video_id = {video.main.id: video for video in es_videos
                                     if hasattr(video, "main") and hasattr(video.main, "id")}

    def _handle_video(self, video_id: str):
        """
        handle the processing of a single video and all of its transcripts
        :param video:
        :return:
        """
        transcripts = self.pg_transcripts_by_video_id.get(video_id, [])
        if not transcripts:
            self.videos_skipped_count += 1
            self.no_pg_transcripts_count += 1
            return

        self.cursor = max([transcript.id for transcript in transcripts])

        transcripts = [transcript for transcript in transcripts if transcript.transcript]
        if not transcripts:
            self.videos_skipped_count += 1
            self.empty_pg_transcripts_count += 1
            return

        es_video = self.es_video_by_video_id.get(video_id, None)
        if es_video is None:
            self.videos_skipped_count += 1
            self.no_es_record_count += 1
            return

        es_video.populate_custom_captions(items=[{}], has_transcripts=True)
        self.video_upsert_queue.append(es_video)

        custom_transcripts = []
        tts_url_transcripts = []
        watson_transcripts = []
        transcripts_by_type = {
            AuditVideoTranscriptSourceTypeIdEnum.CUSTOM.value: custom_transcripts,
            AuditVideoTranscriptSourceTypeIdEnum.TTS_URL.value: tts_url_transcripts,
            AuditVideoTranscriptSourceTypeIdEnum.WATSON.value: watson_transcripts
        }
        for transcript in transcripts:
            transcript_list = transcripts_by_type.get(transcript.source, None)
            if transcript_list is None:
                continue
            transcript_list.append(transcript)

        # get the top 5 custom transcripts per video id
        # re-process custom captions
        transcripts_to_create = []
        custom_transcripts_by_language = self._get_transcripts_by_language_map(custom_transcripts)
        custom_transcripts_to_create = self._get_top_n_transcripts(custom_transcripts_by_language, limit=5)
        if len(custom_transcripts_to_create):
            transcripts_to_create.extend(custom_transcripts_to_create)
            self.custom_transcripts_count += len(custom_transcripts_to_create)

        # re-process tts url captions, should only get the top tts url transcript
        tts_url_transcripts_by_language = self._get_transcripts_by_language_map(tts_url_transcripts)
        tts_url_transcripts_to_create = self._get_top_n_transcripts(tts_url_transcripts_by_language, limit=1)
        if len(tts_url_transcripts_to_create):
            transcripts_to_create.extend(tts_url_transcripts_to_create)
            self.tts_url_transcripts_count += len(transcripts_to_create)

        # re-add the watson transcript, there should only be one
        english = self._get_english_language_instance()
        watson_transcripts = [transcript for transcript in watson_transcripts if transcript.language_id == english.id]
        if len(watson_transcripts) >= 1:
            watson_transcript = watson_transcripts.pop()
            transcripts_to_create.append(watson_transcript)
            self.watson_transcripts_count += 1

        for pg_transcript in transcripts_to_create:
            soup = BeautifulSoup(pg_transcript.transcript, "xml")
            transcript_text = get_formatted_captions_from_soup(soup)
            es_transcript = Transcript(id=pg_transcript.id)
            es_transcript.populate_video(id=pg_transcript.video.video_id)
            es_transcript.populate_text(value=transcript_text)
            source_type_string = self.source_type_by_id.get(pg_transcript.source)
            es_transcript.populate_general_data(
                language_code=pg_transcript.language.language,
                source_type=source_type_string,
                is_asr=True if source_type_string == TranscriptSourceTypeEnum.TTS_URL.value else False,
                processor_version=PROCESSOR_VERSION,
                processed_at=timezone.now()
            )
            self.transcripts_upsert_queue.append(es_transcript)

        self.videos_processed_count += 1

    def _get_english_language_instance(self):
        """
        lazy getter/setter for english language id
        :return:
        """
        if self.en_language is None:
            self.en_language = AuditLanguage.objects.get(language="en")
        return self.en_language

    @staticmethod
    def _get_transcripts_by_language_map(transcripts: list) -> dict:
        """
        map lang code to AuditVideoTranscript instance
        :param transcripts:
        :return:
        """
        return {transcript.language.language: transcript for transcript in transcripts}

    @staticmethod
    def _get_top_n_transcripts(transcripts_by_languages: dict, limit=5) -> list:
        """
        given a dict that maps a list of transcripts to a lang code, for a particular video, return the top 5 lists of
        transcripts and lang codes as a tuple, ready for the `populate_video_custom_captions` function
        :param transcripts_by_languages:
        :return:
        """
        top_n_transcripts = []
        for language in TRANSCRIPTS_LANGUAGE_PRIORITY:
            transcript = transcripts_by_languages.get(language)
            if transcript:
                top_n_transcripts.append(transcript)
            if len(top_n_transcripts) >= limit:
                break

        return top_n_transcripts

    # exp. backoff w/ noise, intended to catch ES query queue limit exceeded exception
    @backoff(max_backoff=600, exceptions=UPSERT_BACKOFF_EXCEPTIONS)
    def _upsert_videos(self):
        """
        upsert the current upsert queue
        :return:
        """
        if not self.video_upsert_queue:
            return
        self.video_manager.upsert(self.video_upsert_queue)
        self.latest_chunk_video_count = len(self.video_upsert_queue)

    @backoff(max_backoff=600, exceptions=UPSERT_BACKOFF_EXCEPTIONS)
    def _upsert_transcripts(self):
        """
        upsert items in the transcript upsert queue
        :return:
        """
        if not self.video_upsert_queue:
            return
        self.transcript_manager.upsert(self.transcripts_upsert_queue)
        self.latest_chunk_transcripts_ct = len(self.transcripts_upsert_queue)
