import re
import socket

from bs4 import BeautifulSoup as bs
from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils import timezone
import requests
from requests.exceptions import ConnectionError
from threading import Thread
import urllib.parse as urlparse
from urllib.parse import parse_qs

from administration.notifications import send_email
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from utils.lang import replace_apostrophes


class YTTranscriptsScraper(object):
    proxies_file_name = "good_proxies.json"
    NUM_PORTS = 65535
    NUM_THREADS = 100

    PROXY_SERVICE = "backconnect"
    PROXY_MEMBERSHIP = "qe9m"
    PROXY_API_URL = f"http://shifter.io/api/v1/{PROXY_SERVICE}/" \
        f"{PROXY_MEMBERSHIP}/"

    def __init__(self, vid_ids):
        self.vid_ids = vid_ids
        self.vids = []
        self.successful_vids = {}
        self.num_failed_vids = None
        self.failure_reasons = None
        self.available_proxies = []
        self.get_available_proxies()
        self.host = None
        self.port = None
        self.proxy_counter = 0
        self.update_proxy()
        self.create_yt_vids()

    def request_proxy_api(self, endpoint, method="GET", data=None):
        if method == "GET":
            response = requests.get(f"{self.PROXY_API_URL}{endpoint}?api_token={settings.PROXY_API_TOKEN}")
            return response
        elif method == "PUT":
            response = requests.put(f"{self.PROXY_API_URL}{endpoint}?api_token={settings.PROXY_API_TOKEN}",
                                    data=data)
            return response

    def authorize_ip_with_proxy(self):
        ip = requests.get('https://api.ipify.org').text
        endpoint = "authorized-ips"
        method = "PUT"
        response = self.request_proxy_api(endpoint=endpoint, method=method, data={"ips": [ip]})
        return response

    def get_authorized_ips(self):
        endpoint = "authorized-ips"
        response = self.request_proxy_api(endpoint=endpoint)
        return response

    def get_available_proxies(self):
        endpoint = "proxies"
        response = self.request_proxy_api(endpoint=endpoint)
        proxies = response.json()['data']
        self.available_proxies = [
            {
                "host": proxy.split(":")[0],
                "port": proxy.split(":")[1]
            }
            for proxy in proxies
        ]

    def get_geo(self):
        endpoint = "geo"
        response = self.request_proxy_api(endpoint=endpoint)
        return response

    def run_scraper(self):
        # Multithreaded requests
        self.generate_tts_urls()
        self.gather_yt_vids_meta()
        # Multithreaded requests
        self.generate_list_urls()
        self.gather_tts_urls_meta()
        # Multithreaded requests
        self.retrieve_transcripts()
        self.gather_success_and_failures()

    def create_yt_vids(self):
        for vid_id in self.vid_ids:
            yt_vid = YTVideo(vid_id, self)
            self.vids.append(yt_vid)

    def generate_tts_urls(self):
        threads = []
        for vid in self.vids:
            t = Thread(target=vid.generate_tts_url)
            t.start()
            threads.append(t)
            if len(threads) >= self.NUM_THREADS:
                for t in threads:
                    t.join()
                threads = []
        for t in threads:
            t.join()

    def gather_yt_vids_meta(self):
        for vid in self.vids:
            vid.parse_yt_vid_meta()

    def generate_list_urls(self):
        threads = []
        for vid in self.vids:
            t = Thread(target=vid.generate_list_url)
            t.start()
            threads.append(t)
            if len(threads) >= self.NUM_THREADS:
                for t in threads:
                    t.join()
                threads = []
        for t in threads:
            t.join()

    def gather_tts_urls_meta(self):
        for vid in self.vids:
            vid.parse_tts_url_meta()

    def retrieve_transcripts(self):
        threads = []
        for vid in self.vids:
            t = Thread(target=vid.generate_subtitles)
            t.start()
            threads.append(t)
            if len(threads) >= self.NUM_THREADS:
                for t in threads:
                    t.join()
                threads = []
        for t in threads:
            t.join()

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

    def get_proxy(self):
        return {
            "http": f"{self.host}:{self.port}"
        }

    def update_proxy(self):
        if self.available_proxies:
            self.update_host(host=self.available_proxies[self.proxy_counter]["host"])
            self.update_port(port=self.available_proxies[self.proxy_counter]["port"])
            self.increment_proxy_counter()
        else:
            raise ValidationError("All proxies have been blocked.")

    def increment_proxy_counter(self):
        if len(self.available_proxies) > 0:
            self.proxy_counter = (self.proxy_counter + 1) % len(self.available_proxies)
        else:
            self.proxy_counter = 0

    def update_host(self, host=None):
        if host:
            self.host = host
        else:
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            self.host = ip

    def update_port(self, port=None):
        if port:
            self.port = port
        elif self.port is None:
            self.port = 9150
        else:
            self.increment_port()

    def increment_port(self):
        self.port = (self.port % self.NUM_PORTS) + 1

    def blacklist_and_update_proxy(self, host, port):
        proxy = {
            "host": host,
            "port": port
        }
        if proxy in self.available_proxies:
            self.available_proxies.remove(proxy)
            self.proxy_counter = self.proxy_counter % len(self.available_proxies)
        self.update_proxy()


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

    # Step 1 (Multithreaded)
    def generate_tts_url(self):
        try:
            self.vid_url_response, self.vid_url_status = self.get_vid_url_response(self.vid_url)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 2 (Single-threaded)
    def parse_yt_vid_meta(self):
        try:
            self.set_tts_url()
            self.params = self.parse_tts_url_params(self.tts_url)
            self.subtitles_list_url = self.get_list_url(self.params)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 3 (Multithreaded)
    def generate_list_url(self):
        try:
            self.subtitles_list_url_response, status = self.get_list_url_response(self.subtitles_list_url)
        except Exception as e:
            self.update_failure_reason(e)

    # Step 4 (Single-threaded)
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

    # Step 5 (Multithreaded)
    def generate_subtitles(self):
        try:
            for subtitle in self.subtitles:
                subtitle.get_subtitles()
        except Exception as e:
            self.update_failure_reason(e)

    def get_vid_url_response(self, vid_url):
        try:
            response, status = self.get_response_through_proxy(self.scraper, vid_url, headers=self.YT_HEADERS)
            return response, status
        except Exception as e:
            self.update_failure_reason(e)

    def get_list_url_response(self, list_url):
        if not list_url:
            return None
        try:
            response, status = self.get_response_through_proxy(self.scraper, list_url, headers=self.YT_HEADERS)
            return response, status
        except Exception as e:
            self.update_failure_reason(e)

    def get_response_through_proxy(self, scraper, url, headers=None):
        proxy = scraper.get_proxy()
        host = scraper.host
        port = scraper.port
        scraper.update_proxy()
        # scraper.update_port()
        response = None
        counter = 0
        try:
            # print(f"Sending Request #{counter} to URL: '{url}' through Proxy: '{proxy}'")
            response = requests.get(url=url, proxies=proxy)
            # print(f"Received Response with Status Code: '{response.status_code}' from Proxy: '{proxy}'")
        except ConnectionError:
            pass
        while (not response or response.status_code != 200) and counter < 5:
            counter += 1
            try:
                # print(f"Blacklisting proxy: {proxy}")
                scraper.blacklist_and_update_proxy(host=host, port=port)
                proxy = scraper.get_proxy()
                host = scraper.host
                port = scraper.port
                # print(f"New proxy: {proxy}")
                # print(f"Sending Request #{counter} to URL: '{url}' through Proxy: '{proxy}'")
                response = requests.get(url=url, proxies=proxy)
                # print(f"Received Response with Status Code: '{response.status_code}' from Proxy: '{proxy}'")
            except ConnectionError as e:
                # print(f"Encountered ConnectionError/ProxyError while sending request to '{url}' through Proxy: '{proxy}'."
                #       f"Error message: '{e}'")
                continue
            except ValidationError as e:
                if e.message == "All proxies have been blocked.":
                    self.send_yt_blocked_email()
                    raise e
            except Exception as e:
                raise e
        if counter >= 5:
            raise Exception("Exceeded 5 connection attempts to URL.")
        response_text = response.text
        response_status = response.status_code
        return response_text, response_status

    @staticmethod
    def send_yt_blocked_email():
        subject = "TTS_URL Transcripts Task Proxies Have Been Blocked by Youtube"
        body = f"All TTS_URL Transcripts Proxies have been blocked by Youtube at {timezone.now()}." \
            f"Locking Task for 5 minutes."
        send_email(
            subject=subject,
            from_email=settings.EMERGENCY_SENDER_EMAIL_ADDRESS,
            recipient_list=settings.TTS_URL_TRANSCRIPTS_MONITOR_EMAIL_ADDRESSES,
            html_message=body
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

    def set_tts_url(self):
        if self.vid_url_status != 200:
            return None
        else:
            yt_response_html = self.vid_url_response
            # yt_response_html = yt_response.text()
        if "TTS_URL" not in yt_response_html:
            raise ValidationError("No TTS_URL in Youtube Response.")
        strings = yt_response_html.split("TTS_URL")
        s = strings[1]
        strings = s.split("\"")
        s = strings[1]
        s = s.replace("\\/", "/")
        s = s.replace("\\u0026", "&")
        self.tts_url = s

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

    def get_subtitles(self):
        response, status = self.video.get_response_through_proxy(self.video.scraper, self.subtitle_url,
                                                                 headers=self.video.YT_HEADERS)
        soup = bs(response, "xml")
        captions = replace_apostrophes(" ".join([line.strip() for line in soup.find_all(text=True)])) if soup else ""
        captions = re.sub(r'<font.+?>', '', captions)
        captions = re.sub(r'<\/font>', '', captions)
        captions = captions.replace(".", ". ").replace("?", "? ").replace("!", "! ")
        self.captions = captions
