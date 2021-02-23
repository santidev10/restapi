import json
import logging

from django.core.management import BaseCommand
from django.utils import timezone
from pid import PidFile

from audit_tool.constants import SourceTypeEnum
from audit_tool.models import AuditVideoTranscript
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from utils.transform import populate_video_custom_captions

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("--file_name", help="Name of file with transcripts to import.")

    def handle(self, *args, **kwargs):
        with PidFile(piddir=".", pidname="pull_watson_transcripts.pid"):
            file_name = kwargs["file_name"]
            if not file_name:
                raise Exception("--file_name argument is required.")
            video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                         upsert_sections=(Sections.CUSTOM_CAPTIONS,))
            with open(file_name, "r") as f:
                data = json.load(f)
            vid_transcripts = {}
            for item in data:
                try:
                    transcript_status = item["transcript_status"]
                    if transcript_status == "TRANSCRIPT_READY":
                        url = item["url"]
                        vid_id = url.split("=")[-1]
                        transcript = item["transcript"]
                        vid_transcripts[vid_id] = transcript
                # pylint: disable=broad-except
                except Exception:
                    continue
                # pylint: enable=broad-except
            vid_ids = list(vid_transcripts)
            batch_size = 1000
            batch_index = 0

            while batch_index < len(vid_ids):
                logger.error("Retrieving videos %s to %s.", batch_index, batch_index + batch_size)
                videos_batch = video_manager.get(vid_ids[batch_index:batch_index + batch_size], skip_none=True)
                for video in videos_batch:
                    try:
                        video_id = video.main.id
                        video_transcript = vid_transcripts[video_id]
                        populate_video_custom_captions(video, [video_transcript], ["en"], source="Watson")
                        vid_transcript = AuditVideoTranscript.get_or_create(video_id=video_id, language="en",
                                                                            transcript=video_transcript,
                                                                            source=SourceTypeEnum.WATSON)
                        vid_transcript.submitted = timezone.now()
                        vid_transcript.retrieved = timezone.now()
                        vid_transcript.save(update_fields=["submitted", "retrieved"])
                        logger.error("Stored AuditVideoTranscript for Video with ID: %s.", video_id)
                    # pylint: disable=broad-except
                    except Exception:
                        continue
                    # pylint: enable=broad-except
                logger.error("Upserting %s videos.", len(videos_batch))
                video_manager.upsert(videos_batch)
                logger.error("Upserted %s videos.", len(videos_batch))
                batch_index += batch_size
