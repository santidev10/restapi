import logging
import asyncio
from django.utils import timezone
from datetime import timedelta
from aiohttp import ClientSession
from aiohttp.web import HTTPTooManyRequests
import urllib.parse as urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup as bs
from django.core.exceptions import ValidationError
from django.conf import settings
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from utils.lang import replace_apostrophes
from utils.celery.tasks import lock
from administration.notifications import send_email


logger = logging.getLogger(__name__)
TASK_RETRY_COUNTS = 5
TASK_RETRY_TIME = 30
LOCK_NAME = 'asr_transcripts'


class YTTranscriptsScraper(object):
    BATCH_SIZE = 100

    def __init__(self, vid_ids):
        self.vid_ids = vid_ids
        self.vids = []
        self.successful_vids = {}
        self.num_failed_vids = None
        self.failure_reasons = None

    def run_scraper(self):
        self.create_yt_vids()
        asyncio.run(self.generate_tts_urls())
        self.gather_yt_vids_meta()
        asyncio.run(self.generate_list_urls())
        self.gather_tts_urls_meta()
        asyncio.run(self.retrieve_transcripts())
        self.gather_success_and_failures()

    def create_yt_vids(self):
        for vid_id in self.vid_ids:
            yt_vid = YTVideo(vid_id, self)
            self.vids.append(yt_vid)

    async def generate_tts_urls(self):
        async with ClientSession(trust_env=True) as session:
            await asyncio.gather(*[yt_vid.generate_tts_url(session) for yt_vid in self.vids])

    def gather_yt_vids_meta(self):
        for vid in self.vids:
            vid.parse_yt_vid_meta()

    async def generate_list_urls(self):
        async with ClientSession(trust_env=True) as session:
            await asyncio.gather(*[yt_vid.generate_list_url(session) for yt_vid in self.vids])

    def gather_tts_urls_meta(self):
        for vid in self.vids:
            vid.parse_tts_url_meta()

    async def retrieve_transcripts(self):
        async with ClientSession(trust_env=True) as session:
            await asyncio.gather(*[yt_vid.generate_subtitles(session) for yt_vid in self.vids])

    def gather_success_and_failures(self):
        for yt_vid in self.vids:
            if yt_vid.failure_reason is not None:
                continue
            vid_id = yt_vid.vid_id
            if yt_vid.vid_url_status != 200:
                yt_vid.failure_reason = f"Failed to get response from Youtube for Video: '{vid_id}'. " \
                    f"Received status code: '{yt_vid.vid_url_status}' from URL '{yt_vid.vid_url}'"
            elif not yt_vid.tts_url:
                yt_vid.failure_reason = f"No TTS_URL for Video: '{vid_id}'."
            elif not yt_vid.subtitles_list_url:
                yt_vid.failure_reason = f"No TRACKS_LIST_URL found for Video: '{vid_id}'."
            elif not yt_vid.tracks_meta:
                yt_vid.failure_reason = f"Video: '{vid_id}' has no TTS_URL captions available."

            if yt_vid.failure_reason is None:
                self.successful_vids[vid_id] = yt_vid
        self.failure_reasons = {vid.vid_id: vid.failure_reason for vid in self.vids if vid.failure_reason}
        self.num_failed_vids = len(self.failure_reasons)


class YTVideo(object):
    YT_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.7",
        "User-Agent": "Mozilla/5.0 (compatible; Google2SRT/0.7.8)"
    }
    NUM_SUBTITLES_TO_PULL = 5

    def __init__(self, vid_id, scraper):
        self.vid_id = vid_id
        self.scraper = scraper
        self.vid_url = self.get_vid_url(self.vid_id)
        self.vid_url_response = None
        self.vid_url_status = None
        self.tts_url = None
        self.params = None
        self.subtitles_list_url = None
        self.subtitles_list_url_response = None
        self.tracks_meta = None
        self.asr_track_meta = None
        self.asr_lang_code = None
        self.tracks_lang_codes_dict = None
        self.top_lang_codes = None
        self.top_subtitles_meta = None
        self.subtitles = None
        self.failure_reason = None

    def update_failure_reason(self, e):
        if not self.failure_reason:
            self.failure_reason = e

    # Step 1 (Asynchronous)
    async def generate_tts_url(self, session):
        try:
            self.vid_url_response, self.vid_url_status = await self.get_vid_url_response(session, self.vid_url)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 2 (Synchronous)
    def parse_yt_vid_meta(self):
        try:
            self.tts_url = self.get_tts_url(self.vid_url_response)
            self.params = self.parse_tts_url_params(self.tts_url)
            self.subtitles_list_url = self.get_list_url(self.params)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 3 (Asynchronous)
    async def generate_list_url(self, session):
        try:
            self.subtitles_list_url_response, status = await self.get_list_url_response(session, self.subtitles_list_url)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 4 (Synchronous)
    def parse_tts_url_meta(self):
        try:
            self.tracks_meta = self.parse_list_url(self.subtitles_list_url_response)
            self.asr_track_meta = self.get_asr_track(self.tracks_meta)
            self.asr_lang_code = self.asr_track_meta.get("lang_code")
            self.tracks_lang_codes_dict = self.get_lang_codes_dict(self.tracks_meta)
            self.top_lang_codes, self.top_subtitles_meta = self.get_top_subtitles_meta()
            self.subtitles = self.get_top_subtitles()
        except Exception as e:
            self.update_failure_reason(e)

    # Step 5 (Asynchronous)
    async def generate_subtitles(self, session):
        try:
            for subtitle in self.subtitles:
                await subtitle.get_subtitles(session)
        except Exception as e:
            self.update_failure_reason(e)

    async def get_vid_url_response(self, session, vid_url):
        try:
            response, status = await self.get_response_async(session, vid_url, headers=self.YT_HEADERS)
            return response, status
        except Exception as e:
            self.update_failure_reason(e)

    async def get_list_url_response(self, session, list_url):
        if not list_url:
            return None
        try:
            response, status = await self.get_response_async(session, list_url, headers=self.YT_HEADERS)
            return response, status
        except Exception as e:
            self.update_failure_reason(e)

    async def get_response_async(self, session, url, headers=None):
        response = None
        counter = 0
        print(f"Sending Request #{counter} to URL: '{url}'")
        response = await session.get(url=url, headers=headers)
        print(f"Received Response with Status Code: '{response.status}'")
        while (not response or response.status != 200) and counter < 5:
            counter += 1
            try:
                print(f"Sending Request #{counter} to URL: '{url}")
                response = await session.get(url=url, headers=headers)
                print(f"Received Response with Status Code: '{response.status}'")
            except HTTPTooManyRequests:
                await asyncio.sleep(30)
                logger.debug(f"Transcript request for url: {url} Attempt #{counter} of {TASK_RETRY_COUNTS} failed."
                             f"Sleeping for {TASK_RETRY_TIME} seconds.")
        if counter >= 5:
            lock(LOCK_NAME, max_retries=1, expire=timedelta(hours=24).total_seconds())
            self.send_yt_blocked_email()
            raise HTTPTooManyRequests
        response_text = await response.text()
        response_status = response.status
        return response_text, response_status

    def send_yt_blocked_email(self):
        subject = "TTS_URL Transcripts Task Has Been Blocked by Youtube"
        body = f"TTS_URL Transcripts task has been blocked by Youtube at {timezone.now()}." \
            f"Locking Task for 24 hours."
        send_email(
            subject=subject,
            from_email=settings.EMERGENCY_SENDER_EMAIL_ADDRESS,
            recipient_list=settings.TTS_URL_TRANSCRIPTS_MONITOR_EMAIL_ADDRESSES,
            html_message=body,
        )

    def get_top_subtitles(self):
        if not self.top_subtitles_meta:
            return None
        top_subtitles = []
        for subtitle_meta in self.top_subtitles_meta:
            subtitle_options = {
                "video": self,
                "params": self.params,
                "subtitle_meta": subtitle_meta
            }
            subtitle = YTVideoSubtitles(**subtitle_options)
            top_subtitles.append(subtitle)
        return top_subtitles

    def get_top_subtitles_meta(self):
        best_lang_codes = []
        best_lang_codes_meta = []
        available_lang_codes = set()
        available_lang_codes.update(self.tracks_lang_codes_dict)
        if self.asr_track_meta and self.asr_lang_code:
            best_lang_codes.append(self.asr_lang_code)
            best_lang_codes_meta.append(self.asr_track_meta)
            if self.asr_lang_code in available_lang_codes:
                available_lang_codes.remove(self.asr_lang_code)
        for lang_code in TRANSCRIPTS_LANGUAGE_PRIORITY:
            if len(best_lang_codes) >= self.NUM_SUBTITLES_TO_PULL:
                break
            if lang_code in available_lang_codes:
                best_lang_codes, best_lang_codes_meta = \
                    self.update_lang_code_and_meta(lang_code, best_lang_codes, best_lang_codes_meta)
                available_lang_codes.remove(lang_code)
        while len(best_lang_codes) < self.NUM_SUBTITLES_TO_PULL and available_lang_codes:
            lang_code = available_lang_codes.pop()
            best_lang_codes, best_lang_codes_meta = \
                self.update_lang_code_and_meta(lang_code, best_lang_codes, best_lang_codes_meta)
        return best_lang_codes, best_lang_codes_meta

    def update_lang_code_and_meta(self, lang_code, lang_codes, lang_codes_meta):
        lang_codes.append(lang_code)
        lang_codes_meta.append(self.tracks_lang_codes_dict.get(lang_code))
        return lang_codes, lang_codes_meta

    @staticmethod
    def get_vid_url(vid_id: str):
        return f"http://www.youtube.com/watch?v={vid_id}"

    def get_tts_url(self, yt_response):
        if self.vid_url_status != 200:
            return None
        else:
            yt_response_html = yt_response
        if "TTS_URL" not in yt_response_html:
            raise ValidationError("No TTS_URL in Youtube Response.")
        strings = yt_response_html.split("TTS_URL")
        s = strings[1]
        strings = s.split("\"")
        s = strings[1]
        s = s.replace("\\/", "/")
        s = s.replace("\\u0026", "&")
        return s

    @staticmethod
    def parse_tts_url_params(tts_url: str):
        if not tts_url:
            return None
        parsed_url = urlparse.urlparse(tts_url)
        params_dict = parse_qs(parsed_url.query)
        params_dict = {key: value[0] for key, value in params_dict.items()}
        return params_dict

    @staticmethod
    def get_list_url(params: dict):
        if not params:
            return None
        s = "https://www.youtube.com/api/timedtext?"
        for key, value in params.items():
            s += f"{key}={value}&"
        s += "asrs=1&type=list&tlangs=1"
        return s

    def parse_list_url(self, list_url_response):
        if not list_url_response:
            raise ValidationError("No list_url found.")
        soup = bs(list_url_response, 'xml')
        transcript_list = soup.transcript_list
        docid = transcript_list.attrs.get('docid')
        if not docid:
            raise ValidationError("list_url has no 'docid' attribute.")
        tracks = self.get_tracks(soup)
        num_tracks = len(tracks)
        if num_tracks < 1:
            raise ValidationError("list_url has no tracks.")
        return tracks

    @staticmethod
    def get_tracks(soup: bs):
        return [track for track in soup.find_all("track")]

    @staticmethod
    def get_asr_track(tracks):
        asr_track = [track for track in tracks if track.get("kind") == "asr"]
        return asr_track[0] if asr_track else {}

    @staticmethod
    def get_lang_codes_dict(items):
        return {item.get("lang_code"): item for item in items if item.get("lang_code")}


class YTVideoSubtitles(object):
    def __init__(self, video, params=None, subtitle_meta=None):
        self.video = video
        self.params = params
        self.subtitle_meta = subtitle_meta
        self.subtitle_id = None
        self.name = None
        self.is_asr = None
        self.lang_code = None
        self.lang_original = None
        self.lang_translated = None
        self.type = None
        self.subtitle_url = None
        self.captions = None
        self.set_meta_data()

    def set_meta_data(self):
        self.subtitle_id = self.subtitle_meta.get('id')
        self.name = self.subtitle_meta.get('name')
        self.is_asr = True if self.subtitle_meta.get('kind') == 'asr' else False
        self.lang_code = self.subtitle_meta.get('lang_code')
        self.lang_original = self.subtitle_meta.get('lang_original')
        self.lang_translated = self.subtitle_meta.get('lang_translated')
        self.type = self.subtitle_meta.name
        self.subtitle_url = self.get_subtitle_url()

    def is_track(self):
        return self.type == "track"

    def get_subtitle_url(self):
        subtitle_url = "https://www.youtube.com/api/timedtext?"
        for key, value in self.params.items():
            subtitle_url += f"{key}={value}&"
        subtitle_url += f"name={self.name}&lang={self.lang_code}&type=track"
        if self.is_asr:
            subtitle_url += "&kind=asr"
        return subtitle_url

    async def get_subtitles(self, session):
        response, status = await self.video.get_response_async(session, self.subtitle_url)
        soup = bs(response, "xml")
        captions = replace_apostrophes(" ".join([line.strip() for line in soup.find_all(text=True)])) if soup else ""
        captions = captions.replace(".", ". ").replace("?", "? ").replace("!", "! ")
        self.captions = captions
