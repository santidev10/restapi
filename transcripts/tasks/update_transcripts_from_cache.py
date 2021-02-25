import logging
import random
import sys
from time import sleep
from typing import Iterable
from typing import Tuple

from bs4 import BeautifulSoup
from celery.exceptions import Retry
from datetime import timedelta
from django.conf import settings
from django.utils import timezone
from elasticsearch.helpers.errors import BulkIndexError
from http.client import IncompleteRead
from urllib3.exceptions import ConnectionError as Urllib3ConnectionError
from urllib3.exceptions import ProtocolError
from elasticsearch.exceptions import ConnectionError

from administration.notifications import send_email
from audit_tool.constants import AuditVideoTranscriptSourceTypeEnum
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditVideoTranscript
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from transcripts.utils import get_formatted_captions_from_soup
from utils.celery.tasks import lock
from utils.exception import backoff
from utils.transform import populate_video_custom_captions
from utils.utils import chunked_queryset


logger = logging.getLogger(__name__)


# the greatest extant AuditVideoTranscript ID after VIQ2-643 was released
TRANSCRIPTS_UPDATE_ID_CEILING = 45831922


class TranscriptsFromCacheUpdater:

    CHUNK_SIZE = 5000
    SLEEP_SECONDS = 0
    EMAIL_LOCK_NAME = "update_transcripts_from_cache_email"
    EMAIL_LIST = ["andrew.wong@channelfactory.com"]

    def __init__(self):
        self.cursor = 0
        self.es_videos_map = {}
        self.pg_transcript_map = {}
        self.upsert_queue = []
        self.floor = 0
        self.ceiling = 0
        self.counts_by_lang_code = {}
        self.skipped_count = 0
        self.videos_processed_count = 0
        self.no_es_record_count = 0
        self.no_pg_transcripts_count = 0
        self.transcripts_to_process_count = 0
        self.empty_pg_transcripts_count = 0
        self.custom_transcripts_count = 0
        self.watson_transcripts_count = 0
        self.tts_url_transcripts_count = 0
        # chunking
        self.chunks_count = 0
        self.latest_chunk_dur_seconds = 0
        self.average_chunk_dur_seconds = 0
        self.en_language = None
        # self.manager = self._get_manager_instance()

    def run(self, floor: int = 0, ceiling: int = TRANSCRIPTS_UPDATE_ID_CEILING):
        """
        main interface for running update script. optionally specify an inclusive floor from which to start, and an
        inclusive ceiling at which to end
        :param floor:
        :param ceiling:
        :return:
        """
        self.floor = floor
        self.ceiling = ceiling
        query = AuditVideoTranscript.objects.prefetch_related("video", "language")\
            .filter(id__gte=floor, id__lte=ceiling)\
            .order_by("id")
        self.transcripts_to_process_count = query.count()
        for chunk in chunked_queryset(query, self.CHUNK_SIZE):
            start = timezone.now()
            self._handle_videos_chunk(chunk)
            self.latest_chunk_dur_seconds = (timezone.now() - start).total_seconds()
            self.chunks_count += 1
            self.average_chunk_dur_seconds = running_average(count=self.chunks_count,
                                                             value=self.latest_chunk_dur_seconds,
                                                             average=self.average_chunk_dur_seconds)
            self._report()

            if self.SLEEP_SECONDS:
                logger.info(f"sleeping for {self.SLEEP_SECONDS}")
                sleep(self.SLEEP_SECONDS)

    def _handle_videos_chunk(self, chunk: Iterable):
        """
        handle a chunk of videos
        :param chunk:
        :return:
        """
        chunk_length = len(chunk)
        if chunk_length != self.CHUNK_SIZE:
            logger.info(f"RECURSING chunk of size: {chunk_length}")
        try:
            self._map_es_videos(chunk)
        except (ConnectionError, Urllib3ConnectionError, IncompleteRead, ProtocolError) as e:
            # problem video within chunk? or chunk too large?
            logger.info(f"caught exception of type: {type(e).__module__}.{type(e).__qualname__}")
            # TODO remove, debug
            video_lengths = {}
            video_sizes = {}
            print(f"video ids: {[video.video.video_id for video in chunk]}")
            for video in chunk:
                video_id = video.video.video_id
                length = video_lengths.get(video_id, 0)
                length += len(video.transcript)
                video_lengths[video_id] = length
                size = video_sizes.get(video_id, 0)
                size += sys.getsizeof(video.transcript)
                video_sizes[video_id] = size
            video_lengths = dict(sorted(video_lengths.items(), key=lambda item: item[1], reverse=True))
            total_length = sum(video_lengths.values())
            total_size = sum(video_sizes.values())
            print(f"total length: {total_length}, total size: {total_size}")
            counter = 0
            for id, length in video_lengths.items():
                print(f"{id}: length - {length}  |  size: {video_sizes.get(id)} bytes")
                counter += 1
                if counter > 25:
                    break

            if chunk_length < 2:
                logger.info(f"RECURSED TO PROBLEM VIDEO: {chunk[0].video.video_id}")
                return
            # split in two and recurse until we find the problem video
            divisor = round(chunk_length / 2)
            for half_chunk in [chunk[:divisor], chunk[divisor:]]:
                sleep(5)
                self._handle_videos_chunk(half_chunk)
            return
        except Exception as e:
            logger.warning(f"CAUGHT BROAD EXCEPTION OF TYPE: {type(e).__module__}.{type(e).__qualname__}")
            print(e)
            return

        # we need to map a transcript to video id to handle multiple languages/transcripts per video
        self._map_pg_transcripts(chunk)

        video_ids = self.pg_transcript_map.keys()
        for video_id in video_ids:
            self._handle_video(video_id)

        self._upsert_chunk()
        self.upsert_queue = []

    def _report(self):
        """
        reports current progress
        :return:
        """
        total_pct = round((self.cursor / self.ceiling) * 100, 2)
        transcripts_this_run = self.cursor - self.floor
        runtime_pct = round((transcripts_this_run / self.ceiling) * 100, 2)
        transcripts_processed_count = sum([
            self.custom_transcripts_count,
            self.watson_transcripts_count,
            self.tts_url_transcripts_count
        ])
        custom_pct = round((self.custom_transcripts_count / transcripts_processed_count) * 100, 2)
        tts_url_pct = round((self.tts_url_transcripts_count / transcripts_processed_count) * 100, 2)
        watson_pct = round((self.watson_transcripts_count / transcripts_processed_count) * 100, 2)

        skipped_pct = round((self.skipped_count / self.transcripts_to_process_count) * 100, 2)
        no_es_record_pct = round((self.no_es_record_count / self.transcripts_to_process_count) * 100, 2)
        no_pg_transcripts_pct = round((self.no_pg_transcripts_count / self.transcripts_to_process_count) * 100, 2)
        empty_pg_transcripts_pct = round((self.empty_pg_transcripts_count / self.transcripts_to_process_count) * 100,
                                         2)

        chunks_remaining_count = round((self.ceiling - self.cursor) / self.CHUNK_SIZE)
        eta_seconds = round(self.average_chunk_dur_seconds * chunks_remaining_count)
        message = (
            "\n"
            f"total transcripts progress: {total_pct}% (cursor: {self.cursor} ceiling: {self.ceiling}) \n"
            f"----- chunks processed this run: {self.chunks_count} \n"
            f"----- latest chunk duration: {timedelta(seconds=self.latest_chunk_dur_seconds)} \n"
            f"----- average chunk duration: {timedelta(seconds=self.average_chunk_dur_seconds)} \n"
            f"----- chunks to completion: {chunks_remaining_count} \n"
            f"----- approx. time to completion: {timedelta(seconds=eta_seconds)}\n"
            f"videos processed this run: {self.videos_processed_count} \n"
            f"videos skipped this run: {self.skipped_count} ({skipped_pct}%) \n"
            f"----- no es record: {self.no_es_record_count} ({no_es_record_pct}%) \n"
            f"----- no PG transcripts: {self.no_pg_transcripts_count} ({no_pg_transcripts_pct}%) \n"
            f"----- empty PG transcripts: {self.empty_pg_transcripts_count} ({empty_pg_transcripts_pct}%) \n"
            f"total transcripts remaining: {self.transcripts_to_process_count - self.cursor} \n"
            f"transcripts processed this run: {transcripts_this_run} ({runtime_pct})% \n"
            f"----- custom transcripts: {self.custom_transcripts_count} ({custom_pct}% of processed) \n"
            f"----- tts_url transcripts: {self.tts_url_transcripts_count} ({tts_url_pct}% of processed) \n"
            f"----- watson transcripts: {self.watson_transcripts_count} ({watson_pct}% of processed) \n"
            f"transcript languages: \n"
        )
        sorted_counts = dict(sorted(self.counts_by_lang_code.items(), key=lambda item: item[1], reverse=True))
        for code, count in sorted_counts.items():
            message += f"----- {code}: {count} \n"
        logger.info(message)

        try:
            lock(lock_name=self.EMAIL_LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Retry:
            return

        subject = f"Update transcripts from cache progress: ({total_pct}%)"
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
        self.pg_transcript_map = {}
        for transcript in chunk:
            video_id = transcript.video.video_id
            transcripts = self.pg_transcript_map.get(video_id, [])
            transcripts.append(transcript)
            self.pg_transcript_map[video_id] = transcripts

        # we need to get all priority transcripts for a video, in order to figure out top five transcripts by language
        video_ids = []
        for video_id, transcripts in self.pg_transcript_map.items():
            if len(transcripts) > 1:
                video_ids.append(video_id)
        # we're done here if no additional transcripts to process
        if not len(video_ids):
            return

        # add transcripts to the map
        transcripts = self._get_all_transcripts_by_video_ids(video_ids=video_ids)
        for transcript in transcripts:
            video_id = transcript.video.video_id
            transcript_items = self.pg_transcript_map.get(video_id, [])
            if transcript in transcript_items:
                continue
            transcript_items.append(transcript)
            self.pg_transcript_map[video_id] = transcript_items

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
        logger.info(f"requesting {len(video_ids)} videos from ES")
        manager = self._get_manager_instance()
        es_videos = manager.get(video_ids)
        # es_videos = self.manager.get(video_ids)
        self.es_videos_map = {video.main.id: video for video in es_videos
                              if hasattr(video, "main") and hasattr(video.main, "id")}

    def _handle_video(self, video_id: str):
        """
        handle the processing of a single video and all of its transcripts
        :param video:
        :return:
        """
        transcripts = self.pg_transcript_map.get(video_id, [])
        if not transcripts:
            self.skipped_count += 1
            self.no_pg_transcripts_count += 1
            return

        self.cursor = max([transcript.id for transcript in transcripts])

        transcripts = [transcript for transcript in transcripts if transcript.transcript]
        if not transcripts:
            self.skipped_count += 1
            self.empty_pg_transcripts_count += 1
            return

        es_video = self.es_videos_map.get(video_id, None)
        if es_video is None:
            self.skipped_count += 1
            self.no_es_record_count += 1
            return

        custom_transcripts = []
        tts_url_transcripts = []
        watson_transcripts = []
        transcripts_by_type = {
            AuditVideoTranscriptSourceTypeEnum.CUSTOM.value: custom_transcripts,
            AuditVideoTranscriptSourceTypeEnum.TTS_URL.value: tts_url_transcripts,
            AuditVideoTranscriptSourceTypeEnum.WATSON.value: watson_transcripts
        }
        for transcript in transcripts:
            transcript_list = transcripts_by_type.get(transcript.source, None)
            if transcript_list is None:
                continue
            transcript_list.append(transcript)

        # whether or not to append to custom_captions.items. don't want to drastically change how
        # populate_video_custom_captions works. It only takes one "source" argument, so we can only pass homogenously
        # sourced items
        append = False

        # get the top 5 custom transcripts per video id
        # re-process custom captions
        custom_transcripts_by_language = self._get_transcripts_by_language_map(custom_transcripts)
        transcript_texts, lang_codes = self._get_top_5_transcripts_and_languages(custom_transcripts_by_language)
        if len(lang_codes) and len(transcript_texts) == len(lang_codes):
            populate_video_custom_captions(es_video, transcript_texts=transcript_texts, transcript_languages=lang_codes,
                                           source="timedtext")
            for code in lang_codes:
                self._increment_language_count(code)
            self.custom_transcripts_count += len(lang_codes)
            append = True

        # re-process tts url captions, should only get the top tts url transcript
        tts_url_transcripts_by_language = self._get_transcripts_by_language_map(tts_url_transcripts)
        transcript_texts, lang_codes = self._get_top_5_transcripts_and_languages(tts_url_transcripts_by_language)
        if len(lang_codes) and len(transcript_texts) == len(lang_codes):
            populate_video_custom_captions(es_video, transcript_texts=transcript_texts[:1],
                                           transcript_languages=lang_codes[:1], source="tts_url",
                                           asr_lang=lang_codes[0], append=append)
            self._increment_language_count(lang_codes[0])
            self.tts_url_transcripts_count += 1
            append = True

        # re-add the watson transcript, there should only be one
        english = self._get_english_language_instance()
        watson_transcripts = [transcript for transcript in watson_transcripts if transcript.language_id == english.id]
        if len(watson_transcripts) >= 1:
            watson_transcript = watson_transcripts.pop()
            populate_video_custom_captions(es_video, transcript_texts=[watson_transcript.transcript],
                                           transcript_languages=[watson_transcript.language.language],
                                           source="Watson", append=append)
            self._increment_language_count(watson_transcript.language.language)
            self.watson_transcripts_count += 1

        self.upsert_queue.append(es_video)
        self.videos_processed_count += 1

    def _increment_language_count(self, code: str):
        """
        increment language count
        :param code:
        :return:
        """
        count = self.counts_by_lang_code.get(code, 0)
        count += 1
        self.counts_by_lang_code[code] = count

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
    def _get_top_5_transcripts_and_languages(transcripts_by_languages: dict) -> Tuple[list, list]:
        """
        given a dict that maps a list of transcripts to a lang code, for a particular video, return the top 5 lists of
        transcripts and lang codes as a tuple, ready for the `populate_video_custom_captions` function
        :param transcripts_by_languages:
        :return:
        """
        top_5_transcripts_by_language = {}
        for language in TRANSCRIPTS_LANGUAGE_PRIORITY:
            transcript = transcripts_by_languages.get(language)
            if transcript:
                top_5_transcripts_by_language[language] = transcript
            if len(top_5_transcripts_by_language) >= 5:
                break

        # get languages, transcripts as zippable lists
        transcript_texts = []
        lang_codes = []
        for language, transcript in top_5_transcripts_by_language.items():
            soup = BeautifulSoup(transcript.transcript, "xml")
            transcript_text = get_formatted_captions_from_soup(soup)
            transcript_texts.append(transcript_text)
            lang_codes.append(language)

        return transcript_texts, lang_codes

    # exp. backoff w/ noise, intended to catch ES query queue limit exceeded exception
    @backoff(max_backoff=120, exceptions=(BulkIndexError,))
    def _upsert_chunk(self):
        """
        upsert the current upsert queue
        :return:
        """
        manager = self._get_manager_instance()
        manager.upsert(self.upsert_queue)
        # self.manager.upsert(self.upsert_queue)

    @staticmethod
    def _get_manager_instance():
        return VideoManager(sections=(Sections.MAIN,), upsert_sections=(Sections.CUSTOM_CAPTIONS,))


def running_average(count: int, value: float, average: float):
    """
    get a running average
    :param count:
    :param value:
    :param average:
    :return:
    """
    average += (value - average) / count
    return average

def recurse_proof_of_concept(chunk: list = None, size=100):
    """
    proof of concept for recursive try/except on ES get requests
    :param chunk:
    :param size:
    :return:
    """
    if not chunk:
        chunk = list(range(size))
        occurances_count = random.randint(1, round(size / 10))
        print(f"occurances count: {occurances_count}")
        for _ in range(occurances_count):
            chunk.append("x")
        random.shuffle(chunk)

    chunk_length = len(chunk)
    print(f"chunk len: {chunk_length}")
    print(chunk)

    if "x" in chunk:
        if chunk_length < 2:
            print(f"found x! {chunk[0]}")
            return
        divisor = round(chunk_length / 2)
        first = chunk[:divisor]
        last = chunk[divisor:]
        for item in [first, last]:
            recurse_proof_of_concept(item)
