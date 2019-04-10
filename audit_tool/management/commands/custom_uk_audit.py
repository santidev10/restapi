import csv
import logging
from io import BytesIO
from typing import Dict
from typing import List

import xlsxwriter
from django.conf import settings
from django.core.management import BaseCommand
from django.http import QueryDict

from audit_tool.dmo import VideoDMO
from audit_tool.keywords import Keywords
from audit_tool.youtube import Youtube
from brand_safety.models import BadWord
from singledb.connector import SingleDatabaseApiConnector
import re
import requests

logger = logging.getLogger(__name__)


class AuditUK():
    channels = {}
    bad_channels = []
    keywords = []
    _regexp = None
    channel_ids = []
    done_channels = []

    def load_keywords(self):
        with open( "blacklist.csv", "r", encoding = 'latin-1') as blacklist:
            reader = csv.reader(blacklist)
            for row in reader:
                self.keywords.append(row[0].lower())
        blacklist.close()
        regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w)) for w in self.keywords])
        )
        self._regexp = re.compile(regexp)

    def load_channels(self):
        with open( "channels.csv", "r", encoding = 'latin-1') as channels_list:
            reader = csv.reader(channels_list)
            for row in reader:
                channel_id = row[1].split("/")[-1]
                self.channels[channel_id] = row
        channels_list.close()

    def check_blacklist(text):
        keywords = re.findall(_regexp, text.lower())
        if len(keywords) > 0: # we found 1 or more bad words
            return True
        return False

    def check_channel_titles(self):
        for channel_id, channel_data in self.channels.items():
            title = channel_data[0].lower()
            if self.check_blacklist(title):
                self.bad_channels.append(channel_id)

    def get_channel_meta(self):
        channel_ids = []
        for channel_id, channel_data in self.channels.items():
            if channel_id not in self.bad_channels and channel_id not in self.done_channels:
                channel_ids.append(channel_id)
                if len(channel_ids) >= 50:
                    self.process_yt_requests(channel_ids)
                    channel_ids = []
        if len(channel_ids) > 0:
            self.process_yt_requests(channel_ids)

    def process_yt_requests(self, channel_ids):
        DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
        DATA_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                       "?key={key}&part=id,brandingSettings&id={ids}"
        ids_str = ",".join(channel_ids)
        url = DATA_API_URL.format(key=DATA_API_KEY, ids=ids_str)
        r = requests.get(url)
        data = r.json()
        for i in data['items']:
            description = i['brandingSettings']['channel'].get('description')
            tags = i['brandingSettings']['channel'].get('keywords')
            self.done_channels.append(i['id'])
            if tags and self.check_blacklist(tags):
                self.bad_channels.append(i['id'])
            elif description and self.check_blacklist(description):
                self.bad_channels.append(i['id'])

    def create_output_file(self):
        with open('clean_export.csv', 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for channel_id, channel_data in self.channels:
                if channel_id not in self.bad_channels:
                    wr.writerow(channel_data)
            myfile.close()