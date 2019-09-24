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

from es_components.connections import init_es_connection


class Command(BaseCommand):

    def handle(self, *args, **options):
        init_es_connection()
        with PidFile(piddir='.', pidname='pull_transcripts.pid') as p:
            unparsed_vids = self.get_unparsed_vids()


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
                    "transcript_checked": False
                }
            }
        )

        s = s.query(q1).query(q2).query(q3)
        s = s.sort({"stats.views": {"order": "desc"}})

        for item in s.scan():
            yield item