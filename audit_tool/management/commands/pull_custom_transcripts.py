from django.core.management.base import BaseCommand
import logging
logger = logging.getLogger(__name__)
from pid import PidFile
import requests
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import time
import random

from es_components.connections import init_es_connection
from bs4 import BeautifulSoup as bs
from audit_tool.models import AuditVideoTranscript
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from es_components.constants import Sections
from utils.transform import populate_video_custom_transcripts
from utils.lang import replace_apostrophes


class Command(BaseCommand):

    def handle(self, *args, **options):
        init_es_connection()
        with PidFile(piddir='.', pidname='pull_transcripts.pid') as p:
            unparsed_vids = self.get_unparsed_vids()
            vid_ids = set([vid.main.id for vid in unparsed_vids])
            counter = 0
            transcripts_counter = 0
            video_manager = VideoManager(sections=(Sections.CUSTOM_TRANSCRIPTS,),
                                         upsert_sections=(Sections.CUSTOM_TRANSCRIPTS,))
            for vid_id in vid_ids:
                vid_obj = video_manager.get_or_create([vid_id])[0]
                transcript_soup = self.get_video_soup(vid_id)
                transcript_text = replace_apostrophes(transcript_soup.text) if transcript_soup else ""
                if transcript_text != "":
                    AuditVideoTranscript.get_or_create(video_id=vid_id, language="en", transcript=str(transcript_soup))
                    transcripts_counter += 1
                populate_video_custom_transcripts(vid_obj, [transcript_text], ['en'])
                video_manager.upsert([vid_obj])
                counter += 1
                logger.info("Parsed video with id: {}".format(vid_id))
                logger.info("Number of videos parsed: {}".format(counter))
                logger.info("Number of transcripts retrieved: {}".format(transcripts_counter))

    def get_video_soup(self, vid_id):
        transcript_url = "http://video.google.com/timedtext?lang=en&v="
        vid_transcript_url = transcript_url + vid_id
        transcript_response = requests.get(vid_transcript_url)
        if transcript_response.status_code == 200:
            soup = bs(transcript_response.text, "xml")
            return soup
        else:
            return None

    def get_unparsed_vids(self):
        s = Search(using='default')
        s = s.index(Video.Index.name)

        # Get English Videos Query
        q1 = Q(
            {
                "term": {
                    "general_data.language": {
                        "value": "English"
                    }
                }
            }
        )
        # Get Videos with no captions
        q2 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "captions"
                        }
                    }
                }
            }
        )
        # Get Videos with no custom_transcripts.transcripts_checked
        q3 = Q(
            {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "custom_transcripts.transcripts_checked"
                        }
                    }
                }
            }
        )
        s = s.query(q1).query(q2).query(q3)
        s = s.sort({"stats.views": {"order": "desc"}})
        s = s[:10000]
        return s.execute()
