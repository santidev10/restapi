import requests
import urllib.parse as urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup as bs
import json
import re
import csv
from datetime import datetime
import sys
from django.core.exceptions import ValidationError


class YTTranscriptsScraper(object):
    YT_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.7",
        "User-Agent": "Mozilla/5.0 (compatible; Google2SRT/0.7.8)"
    }
    BATCH_SIZE = 100

    def __init__(self, vid_ids):
        self.vid_ids = vid_ids
        self.num_failed_vids = None
        self.failure_reasons = None

    @staticmethod
    def get_tts_url(yt_response: str):
        if "TTS_URL" not in yt_response:
            raise ValidationError("No TTS_URL in Youtube Response.")
        strings = yt_response.split("TTS_URL")
        s = strings[1]
        strings = s.split("\"")
        s = strings[1]
        s = s.replace("\\/", "/")
        s = s.replace("\\u0026", "&")
        return s

    @staticmethod
    def parse_tts_url_params(tts_url: str):
        parsed_url = urlparse.urlparse(tts_url)
        params_dict = parse_qs(parsed_url.query)
        params_dict = {key: value[0] for key, value in params_dict.items()}
        return params_dict

    @staticmethod
    def get_list_url(params: dict):
        s = "https://www.youtube.com/api/timedtext?"
        for key, value in params.items():
            s += f"{key}={value}&"
        s += "asrs=1&type=list&tlangs=1"
        return s

    @staticmethod
    def parse_list_url(xml: bs):
        pass

    def retrieve_transcripts(self):
        failed_vid_reasons = {}
        for vid_id in self.vid_ids:
            try:
                vid_url = f"https://www.youtube.com/watch?v={vid_id}"
                vid_response = requests.get(vid_url, headers=self.YT_HEADERS)
                if vid_response.status_code != 200:
                    failed_vid_reasons[vid_id] = \
                        f"YT response from vid_url:'{vid_url}' returned status_code: {vid_response.status_code}"
                    continue
                tts_url = self.get_tts_url(vid_response.text)
                query_params = self.parse_tts_url_params(tts_url)
                list_url = self.get_list_url(query_params)
                list_url_response = requests.get(list_url, headers=self.YT_HEADERS)
                if list_url_response.status_code != 200:
                    failed_vid_reasons[vid_id] = \
                        f"YT response from list_url:'{list_url}' returned status_code: {list_url_response.status_code}"
                    continue
                xml = bs(list_url_response.text, 'xml')
            except Exception as e:
                failed_vid_reasons[vid_id] = e
                continue
        if len(failed_vid_reasons) > 0:
            self.num_failed_vids = len(failed_vid_reasons)
            self.failure_reasons = failed_vid_reasons


video_ids = ["7L4vIHSz3xo"]
scraper = YTTranscriptsScraper(video_ids)
# vid_id = "7L4vIHSz3xo"
# retrieve_transcripts(vid_id)
