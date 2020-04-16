import asyncio
from aiohttp import ClientSession
from proxyscrape import create_collector
import urllib.parse as urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup as bs
from django.core.exceptions import ValidationError
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from utils.lang import replace_apostrophes


class YTTranscriptsScraper(object):
    BATCH_SIZE = 100

    def __init__(self, vid_ids):
        self.vid_ids = vid_ids
        self.vids = []
        self.num_failed_vids = None
        self.failure_reasons = None
        self.socks_proxies_collector = None
        self.http_proxies_collector = None
        self.collect_proxies()

    def run_scraper(self):
        self.create_yt_vids()
        self.generate_tts_urls()
        self.gather_yt_vids_meta()
        self.generate_list_urls()
        self.retrieve_transcripts()
        self.gather_failed_vid_reasons()

    def create_yt_vids(self):
        for vid_id in self.vid_ids:
            yt_vid = YTVideo(vid_id, self)
            self.vids.append(yt_vid)

    async def generate_tts_urls(self):
        async with ClientSession() as session:
            await asyncio.gather(*[yt_vid.generate_tts_url(session) for yt_vid in self.vids])

    def gather_yt_vids_meta(self):
        for vid in self.vids:
            vid.parse_yt_vid_meta()

    async def generate_list_urls(self):
        async with ClientSession() as session:
            await asyncio.gather(*[yt_vid.generate_list_url(session) for yt_vid in self.vids])

    def gather_tts_urls_meta(self):
        for vid in self.vids:
            vid.parse_tts_url_meta()

    async def retrieve_transcripts(self):
        async with ClientSession() as session:
            await asyncio.gather(*[yt_vid.generate_subtitles(session) for yt_vid in self.vids])
        # for vid in self.vids:
        #     for subtitle in vid.subtitles:
        #         subtitle.get_subtitles()

    def gather_failed_vid_reasons(self):
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
        self.failure_reasons = {vid.vid_id: vid.failure_reason for vid in self.vids}
        self.num_failed_vids = len(self.failure_reasons)

    def collect_proxies(self):
        self.socks_proxies_collector = create_collector('my-collector', ['socks4', 'socks5'])
        self.http_proxies_collector = create_collector('http-collector', ['http', 'https'])

    def get_proxy(self):
        proxy = self.http_proxies_collector.get_proxy() or self.socks_proxies_collector.get_proxy()
        return {proxy.type: f"{proxy.host}:{proxy.port}"}

    def blacklist_proxy(self, proxy):
        if 'http' or 'https' in proxy:
            proxy_url = proxy.get('http') or proxy.get('https')
            host, port = self.get_proxy_host_and_port(proxy_url)
            self.http_proxies_collector.blacklist_proxy(host=host, port=port)
        else:
            proxy_url = proxy.get('socks4') or proxy.get('socks5')
            host, port = self.get_proxy_host_and_port(proxy_url)
            self.socks_proxies_collector.blacklist_proxy(host=host, port=port)

    @staticmethod
    def get_proxy_host_and_port(proxy_url):
        proxy_string = proxy_url.split(":")
        host = proxy_string[0]
        port = proxy_string[1]
        return host, port


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
            self.vid_url_response = self.get_vid_url_response(session, self.vid_url)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 2 (Synchronous)
    def parse_yt_vid_meta(self):
        try:
            self.vid_url_status = self.vid_url_response.status_code
            self.tts_url = self.get_tts_url(self.vid_url_response)
            self.params = self.parse_tts_url_params(self.tts_url)
            self.subtitles_list_url = self.get_list_url(self.params)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 3 (Asynchronous)
    async def generate_list_url(self, session):
        try:
            self.subtitles_list_url_response = self.get_list_url_response(session, self.subtitles_list_url)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 4 (Synchronous)
    def parse_tts_url_meta(self):
        self.tracks_meta = self.parse_list_url(self.subtitles_list_url_response)
        self.asr_track_meta = self.get_asr_track(self.tracks_meta)
        self.asr_lang_code = self.asr_track_meta.get("lang_code")
        self.tracks_lang_codes_dict = self.get_lang_codes_dict(self.tracks_meta)
        self.top_lang_codes, self.top_subtitles_meta = self.get_top_subtitles_meta()
        self.subtitles = self.get_top_subtitles()

    # Step 5 (Asynchronous)
    async def generate_subtitles(self, session):
        try:
            for subtitle in self.subtitles:
                subtitle.get_subtitles(session)
        except Exception as e:
            self.update_failure_reason(e)

    async def get_vid_url_response(self, session, vid_url):
        try:
            response = self.get_response_through_proxy(session, self.scraper, vid_url, headers=self.YT_HEADERS)
            return response
        except Exception as e:
            self.update_failure_reason(e)

    async def get_list_url_response(self, session, list_url):
        if not list_url:
            return None
        try:
            response = self.get_response_through_proxy(session, self.scraper, list_url, headers=self.YT_HEADERS)
            return response
        except Exception as e:
            self.update_failure_reason(e)

    @staticmethod
    async def get_response_through_proxy(session, scraper, url, headers=None):
        proxy = scraper.get_proxy()
        response = None
        try:
            print(f"Sending Request to URL: '{url}' through Proxy: '{proxy}'")
            response = await session.request(method="GET", url=url, proxy=proxy)
            print(f"Received Response with Status Code: '{response.status_code}' from Proxy: '{proxy}'")
        except (ConnectionError, TimeoutError) as e:
            print(f"Encountered error: '{e}' while sending request to '{url}' through Proxy: '{proxy}'")
        while not response or response.status_code != 200:
            try:
                scraper.blacklist_proxy(proxy)
                proxy = scraper.get_proxy()
                print(f"Sending Request to URL: '{url}' through Proxy: '{proxy}'")
                response = await session.request(method="GET", url=url, proxy=proxy)
                print(f"Received Response with Status Code: '{response.status_code}' from Proxy: '{proxy}'")
            except (ConnectionError, TimeoutError) as e:
                print(f"Encountered error: '{e}' while sending request to '{url}' through Proxy: '{proxy}'")
                continue
        return response

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
        return f"https://www.youtube.com/watch?v={vid_id}"

    @staticmethod
    def get_tts_url(yt_response):
        if yt_response.status_code != 200:
            return None
        else:
            yt_response_html = yt_response.text()
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
            return [], []
        soup = bs(list_url_response.text(), 'xml')
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
        self.subtitles = None
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
        response = self.video.get_response_through_proxy(session, self.video.scraper, self.subtitle_url)
        soup = bs(response.text(), "xml")
        subtitles = replace_apostrophes(" ".join([line.strip() for line in soup.find_all(text=True)])) if soup else ""
        subtitles = subtitles.replace(".", ". ").replace("?", "? ").replace("!", "! ")
        self.subtitles = subtitles
