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
            videos = []
            for item in data:
                try:
                    transcript_status = item["transcript_status"]
                    if transcript_status == "TRANSCRIPT_READY":
                        url = item["url"]
                        vid_id = url.split("=")[-1]
                        transcript = item["transcript"]
                        video = video_manager.get([vid_id])
                        if not video:
                            continue
                        populate_video_custom_captions(video, [transcript], ['en'], source="SQ")
                        videos.append(video)
                        AuditVideoTranscript.get_or_create(video_id=vid_id, language='en', transcript=transcript)
                        print(f"Stored AuditVideoTranscript for Video with ID: {vid_id}.")
                except Exception:
                    continue
            video_manager.upsert(videos)
