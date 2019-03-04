import csv
import re
import time
import logging
from segment.models import SegmentChannel
from segment.models import SegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.youtube_api import YoutubeAPIConnector
from django.db.models import Count
from django.db.models import Q, F
from multiprocessing import Process
from multiprocessing import Manager


class CommentAudit(object):
    def __init__(self):
        self.connector = Connector()
        self.youtube_connector = YoutubeAPIConnector()

    def get_comments(self, channel_id: str, page_token):
        results = self.youtube_connector.get_channel_comments(channel_id=channel_id, page_token=page_token)

        return results

    def get_oauth_channels(self):
        pass

    def parse_transcript(self):
        pass

    def get_top_10k_channels(self):
        all_channels = list(PersistentSegmentRelatedChannel.objects.all().distinct().exclude(details__subscribers__isnull=True).values('related_id', 'details'))
        top_10k = sorted(all_channels, key=lambda obj: obj['details'].get('subscribers'), reverse=True)[:10000]

