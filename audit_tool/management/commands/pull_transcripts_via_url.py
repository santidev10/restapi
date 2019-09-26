from django.core.management.base import BaseCommand
import logging
logger = logging.getLogger(__name__)
from pid import PidFile
import requests
import json
import sqlite3
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import time
import random

from es_components.connections import init_es_connection
from bs4 import BeautifulSoup as bs
from audit_tool.models import AuditVideoTranscript
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from es_components.constants import Sections
from utils.transform import populate_video_custom_transcripts


class Command(BaseCommand):

    def handle(self, *args, **options):
        init_es_connection()
        with PidFile(piddir='.', pidname='pull_transcripts.pid') as p:
            unparsed_vids = self.get_unparsed_vids()
            parsed_vids = set()
            counter = 0
            transcripts_counter = 0
            video_manager = VideoManager(sections=(Sections.CUSTOM_TRANSCRIPTS,),
                                         upsert_sections=(Sections.CUSTOM_TRANSCRIPTS,))
            for vid in unparsed_vids:
                vid_id = vid.main.id
                if vid_id in parsed_vids:
                    continue
                else:
                    parsed_vids.add(vid_id)
                vid_obj = video_manager.get([vid_id])[0]
                transcript_soup = self.get_video_soup(vid_id)
                transcript_text = transcript_soup.text
                if transcript_text != "":
                    transcripts_counter += 1
                AuditVideoTranscript.get_or_create(video_id=vid_id, language="en", transcript=transcript_soup)
                # todo: Store transcript_text on Elastic Search Video Model in custom_transcript field, creating a new
                #  VideoCustomTranscript model object, and update custom_transcript.transcript_checked to be True.
                populate_video_custom_transcripts(vid_obj, [transcript_text], ['en'])
                video_manager.upsert(vid_obj)
                counter += 1
                print("Parsed video with id: {}".format(vid_id))
                print("Number of videos parsed: {}".format(counter))
                print("Number of transcripts retrieved: {}".format(transcripts_counter))
                delay = random.choice(range(10, 16))
                time.sleep(delay)

    def get_video_soup(self, vid_id):
        transcript_url = "http://video.google.com/timedtext?lang=en&v="
        vid_transcript_url = transcript_url + vid_id
        transcript_response = requests.get(vid_transcript_url)
        soup = bs(transcript_response.text, "xml")
        return soup

    def get_unparsed_vids(self):
        s = Search(using='default')
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
        # Only get Videos we haven't tried parsing with the URL yet
        q3 = Q(
            {
                "term": {
                    "custom_transcript.transcript_checked": False
                }
            }
        )
        s = s.query(q1).query(q2).query(q3)
        s = s.sort({"stats.views": {"order": "desc"}})
        for video in s.scan():
            yield video
