import logging
import os

from django.conf import settings
from elasticsearch_dsl import Q
from es_components.constants import Sections

from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoTranscript
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from transcripts.tasks.pull_tts_url_transcripts import pull_tts_url_transcripts_with_lock

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

    def run(self):
        cursor = self._get_cursor()
        video_ids = self._get_video_ids_to_update(cursor, settings.TRANSCRIPTS_NUM_VIDEOS)
        if not video_ids:
            logger.info("Transcripts update done!")
            return
        query = self._get_update_transcripts_query(video_ids)
        pull_tts_url_transcripts_with_lock(lock_name=UPDATE_TTS_URL_TRANSCRIPTS_LOCK_NAME, query=query)
        next_video = AuditVideoTranscript.objects.filter(id__gt=cursor).order_by("id").first()
        # next_video = AuditVideo.objects.filter(id__gt=cursor).order_by("id").first()
        self._write_cursor(int(next_video.id))

    @staticmethod
    def _get_update_transcripts_query(video_ids: list):
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

    def _get_video_ids_to_update(self, cursor: int, limit: int) -> list:
        """
        get a list of video ids to update, given a cursor to start at and a limit to define the slice
        :param cursor:
        :param limit:
        :return:
        """
        queryset = AuditVideoTranscript.objects.filter(id__gte=cursor, id__lt=TRANSCRIPTS_UPDATE_ID_CEILING) \
                       .order_by("id") \
                       .values_list("video__video_id", flat=True)[:limit]
        # queryset = AuditVideo.objects.filter(id__gte=cursor, id__lt=TRANSCRIPTS_UPDATE_ID_CEILING) \
        #                .order_by("id") \
        #                .values_list("video_id", flat=True)[:limit]
        return list(queryset)

    def _get_cursor(self) -> int:
        """
        gets the last cursor value from file, or returns the first Video record
        :return:
        """
        exists = os.path.exists(self.CURSOR_FILE)
        if not exists:
            first = AuditVideoTranscript.objects.order_by("id").first()
            # first = AuditVideo.objects.order_by("id").first()
            return int(first.id)

        # get last line in file
        with open(self.CURSOR_FILE, mode="r") as f:
            for line in f:
                pass
            return int(line)

    def _write_cursor(self, cursor: int):
        """
        writes (appends) the given cursor to the self.CURSOR_FILE
        :param cursor:
        :return:
        """
        with open(self.CURSOR_FILE, mode="a") as f:
            f.write("\n")
            f.write(str(cursor))

    def remove_cursor_file(self):
        """
        remove the cursor file
        :return:
        """
        os.remove(self.CURSOR_FILE)
