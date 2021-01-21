import logging
import os
from datetime import timedelta

from celery.exceptions import Retry
from django.conf import settings
from django.db.models import QuerySet
from django.utils import timezone
from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Terms

from administration.notifications import send_email
from audit_tool.models import AuditVideoTranscript
from es_components.constants import Sections
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from transcripts.tasks.pull_tts_url_transcripts import pull_tts_url_transcripts_with_lock
from utils.celery.tasks import lock

logger = logging.getLogger(__name__)

UPDATE_TTS_URL_TRANSCRIPTS_LOCK_NAME = "update_tts_url_transcripts"

# the date when the last transcripts fix (to spacing) was deployed
TRANSCRIPTS_UPDATE_DATE_CEILING = "2021-01-13"
TRANSCRIPTS_UPDATE_DATE_CEILING_FORMAT = "%Y-%m-%d"
# since there are no date timestamps, we will limit the updates by adding an id ceiling
TRANSCRIPTS_UPDATE_ID_CEILING = 45831922


@celery_app.task(expires=TaskExpiration.CUSTOM_TRANSCRIPTS, soft_time_limit=TaskTimeout.CUSTOM_TRANSCRIPTS)
def update_tts_url_transcripts_task():
    """
    updates existing tts url transcripts up to the TRANSCRIPTS_UPDATE_ID_CEILING
    :return:
    """
    updater = TtsUrlTranscriptsUpdater()
    updater.run()


class TtsUrlTranscriptsUpdater:

    CURSOR_FILE = "update_tts_url_transcripts_cursor"
    EMAILER_LOCK_NAME = "update_tts_url_transcripts_emailer_lock"

    def __init__(self):
        self.cursor = self._get_cursor()
        self.video_process_limit = settings.TRANSCRIPTS_NUM_VIDEOS

    def run(self):
        video_ids = self._get_video_ids_to_update()
        if not video_ids:
            self._email_transcripts_update_complete()
            return
        query = self._get_update_transcripts_query(video_ids)
        pull_tts_url_transcripts_with_lock(lock_name=UPDATE_TTS_URL_TRANSCRIPTS_LOCK_NAME, query=query)
        self._fast_forward_cursor_to_last_updated()
        self._write_cursor()
        self._email_progress()

    def _email_transcripts_update_complete(self):
        try:
            lock(lock_name=self.EMAILER_LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Retry:
            return
        subject = "Update TTS URL Transcripts Task COMPLETE!!!"
        body = f"Cursor is at {self.cursor:,} of {TRANSCRIPTS_UPDATE_ID_CEILING:,}. The update is complete!"
        send_email(
            subject=subject,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=settings.TTS_URL_TRANSCRIPTS_MONITOR_EMAIL_ADDRESSES,
            html_message=body
        )

    def _email_progress(self) -> None:
        """ email progress of pointer from ceiling as a percentage of items processed """
        try:
            lock(lock_name=self.EMAILER_LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Retry:
            return
        percentage = round((self.cursor / TRANSCRIPTS_UPDATE_ID_CEILING) * 100, 2)
        now = timezone.now()
        subject = "Update TTS URL Transcripts Task Progress"
        body = f"Cursor is at {self.cursor:,} of {TRANSCRIPTS_UPDATE_ID_CEILING:,} ({percentage}%) as of {now}"
        send_email(
            subject=subject,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=settings.TTS_URL_TRANSCRIPTS_MONITOR_EMAIL_ADDRESSES,
            html_message=body
        )

    @staticmethod
    def _get_update_transcripts_query(video_ids: list) -> Terms:
        """
        gets a query to pass to the pull_tts_url_transcripts function. Simple video by ids query
        :param video_ids:
        :return:
        """
        return Q({
            "terms": {
                f"{Sections.MAIN}.id": video_ids
            }
        })

    def _get_audit_video_transcripts_queryset(self) -> QuerySet:
        """
        returns the queryset for audit video transcripts which is shared by two functions
        :return:
        """
        return AuditVideoTranscript.objects.filter(id__gt=self.cursor, id__lt=TRANSCRIPTS_UPDATE_ID_CEILING) \
            .order_by("id")

    def _fast_forward_cursor_to_last_updated(self):
        """
        updates the cursor position based on the provided limit to the last updated item
        NOTE: only call this after cursor is used! setting this will lose the old cursor position!
        :param cursor:
        :param limit:
        :return:
        """
        queryset = self._get_audit_video_transcripts_queryset()
        self.cursor = max(list(queryset.values_list("id", flat=True)[:self.video_process_limit]))

    def _get_video_ids_to_update(self) -> list:
        """
        get a list of video ids to update, given a cursor to start at and a limit to define the slice
        :param cursor:
        :param limit:
        :return:
        """
        queryset = self._get_audit_video_transcripts_queryset()
        return list(queryset.values_list("video__video_id", flat=True)[:self.video_process_limit])

    def _get_cursor(self) -> int:
        """
        gets the last cursor value from file, or from the first AuditVideoTranscript record
        :return:
        """
        exists = os.path.exists(self.CURSOR_FILE)
        if not exists:
            first = AuditVideoTranscript.objects.order_by("id").first()
            return int(first.id)

        # get last line in file
        with open(self.CURSOR_FILE, mode="r") as f:
            for line in f:
                pass
            return int(line)

    def _write_cursor(self):
        """
        writes (appends) the given cursor to the self.CURSOR_FILE
        :param cursor:
        :return:
        """
        with open(self.CURSOR_FILE, mode="a") as f:
            f.write("\n")
            f.write(str(self.cursor))

    def remove_cursor_file(self):
        """
        remove the cursor file
        :return:
        """
        try:
            os.remove(self.CURSOR_FILE)
        except FileNotFoundError:
            print(f"could not find cursor file '{self.CURSOR_FILE}' to remove")
        else:
            print("removed cursor file successfully")
