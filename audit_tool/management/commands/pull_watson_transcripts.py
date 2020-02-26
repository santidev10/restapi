import logging
import csv
import json
from django.core.management import BaseCommand
from audit_tool.models import AuditVideoTranscript
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from utils.transform import populate_video_custom_captions

from pid.decorator import pidfile
from pid import PidFile


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("--file_name", help="Name of file with transcripts to import.")

    def handle(self, *args, **kwargs):
        with PidFile(piddir=".", pidname="pull_watson_transcripts.pid") as p:
            file_name = kwargs["file_name"]
            video_manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS,),
                                         upsert_sections=(Sections.CUSTOM_CAPTIONS,))
            with open(file_name, "r") as f:
                data = json.load(f)
            vid_transcripts = {}
            videos = []
            for item in data:
                try:
                    transcript_status = item["transcript_status"]
                    if transcript_status == "TRANSCRIPT_READY":
                        url = item["url"]
                        vid_id = url.split("=")[-1]
                        transcript = item["transcript"]
                        vid_transcripts[vid_id] = transcript
                except Exception:
                    continue
            vid_ids = [vid_id for vid_id in vid_transcripts]
            batch_size = 1000
            batch_index = 0

            while batch_index < len(vid_ids):
                logger.error(f"Retrieving videos {batch_index} to {batch_index+batch_size}.")
                videos_batch = video_manager.get(vid_ids[batch_index:batch_index+batch_size], skip_none=True)
                for video in videos_batch:
                    try:
                        video_transcript = vid_transcripts[video.main.id]
                        populate_video_custom_captions(video, [video_transcript], ['en'], source="Watson")
                        AuditVideoTranscript.get_or_create(video_id=vid_id, language='en', transcript=video_transcript,
                                                           source=1)
                        logger.error(f"Stored AuditVideoTranscript for Video with ID: {vid_id}.")
                    except Exception:
                        continue
                logger.error(f"Upserting {len(videos_batch)} videos.")
                video_manager.upsert(videos_batch)
                logger.error(f"Upserted {len(videos_batch)} videos.")
                batch_index += batch_size
