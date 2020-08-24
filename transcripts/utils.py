import re
from threading import Thread

from bs4 import BeautifulSoup as bs
from datetime import timedelta
from django.conf import settings
from django.utils import timezone
import requests
from requests.exceptions import ConnectionError, Timeout

from administration.notifications import send_email
from utils.celery.tasks import lock
from utils.lang import replace_apostrophes


class YTTranscriptsScraper(object):
    EMAILER_LOCK_NAME = "transcripts_alert_emailer"
    NUM_RETRIES = 3
    NUM_THREADS = settings.TRANSCRIPTS_NUM_THREADS
    TIMEOUT = settings.TRANSCRIPTS_TIMEOUT
    PROXY_SERVICE = "backconnect"
    PROXY_MEMBERSHIP = "QBrL"
    PROXY_API_URL = f"http://shifter.io/api/v1/{PROXY_SERVICE}/" \
        f"{PROXY_MEMBERSHIP}/"
    YT_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.7",
        "User-Agent": None
    }

    def __init__(self, vid_ids):
        self.vid_ids = vid_ids
        self.vids = []
        self.successful_vids = {}
        self.num_failed_vids = None
        self.failure_reasons = None
        self.host = settings.PROXY_HOST
        self.port = settings.PROXY_PORT
        self.ua = 'Mozilla/5.0 (compatible; Google2SRT/0.7.8)'
        self.create_yt_vids()

    def run_scraper(self):
        # Multithreaded: Gather Transcripts for All Videos
        self.retrieve_transcripts()
        self.gather_success_and_failures()

    def create_yt_vids(self):
        for vid_id in self.vid_ids:
            yt_vid = YTVideo(vid_id, self)
            self.vids.append(yt_vid)

    def retrieve_transcripts(self):
        threads = []
        for vid in self.vids:
            t = Thread(target=vid.get_captions)
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
            elif not yt_vid.captions_url:
                yt_vid.failure_reason = f"No Captions URL for Video: '{vid_id}'."
            elif not yt_vid.captions:
                yt_vid.failure_reason = f"Failed to retrieve captions for Video: '{vid_id}'."
            if yt_vid.failure_reason is None:
                self.successful_vids[vid_id] = yt_vid
        self.failure_reasons = {vid.vid_id: vid.failure_reason for vid in self.vids if vid.failure_reason}
        self.num_failed_vids = len(self.failure_reasons)

    def get_proxy(self):
        return {
            "http": f"{self.host}:{self.port}",
            "https": f"{self.host}:{self.port}"
        }

    def get_user_agent(self):
        return 'Mozilla/5.0 (compatible; Google2SRT/0.7.8)'

    def get_headers(self):
        headers = self.YT_HEADERS
        headers['User-Agent'] = self.get_user_agent()
        return headers

    def send_yt_blocked_email(self):
        try:
            lock(lock_name=self.EMAILER_LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Exception as e:
            return
        subject = "ASR Transcripts Task Proxies Have Been Blocked by Youtube"
        body = f"All ASR Transcripts Proxies have been blocked by Youtube at {timezone.now()}." \
            f"Locking Task for 5 minutes."
        send_email(
            subject=subject,
            from_email=settings.EMERGENCY_SENDER_EMAIL_ADDRESS,
            recipient_list=settings.TTS_URL_TRANSCRIPTS_MONITOR_EMAIL_ADDRESSES,
            html_message=body
        )


class YTVideo(object):
# pylint: disable=too-many-instance-attributes
    NUM_SUBTITLES_TO_PULL = 5

    def __init__(self, vid_id, scraper):
        self.vid_id = vid_id
        self.scraper = scraper
        self.vid_url = self.get_vid_url(self.vid_id)
        self.vid_url_status = None
        self.captions_url = None
        self.captions_url_response = None
        self.captions_url_status = None
        self.captions = None
        self.captions_language = None
        self.failure_reason = None

    def update_failure_reason(self, e):
        if not self.failure_reason:
            self.failure_reason = e

    @staticmethod
    def get_vid_url(vid_id: str):
        return f"http://www.youtube.com/watch?v={vid_id}"

    @staticmethod
    def clean_url(url):
        url = url.replace("\\", "")
        url = url.replace("u0026", "&")
        return url

    # New Captions Method
    def get_captions(self):
        try:
            vid_response, self.vid_url_status = self.get_response_through_proxy(self.scraper, self.vid_url)
            captions_url = vid_response.split("playerCaptionsTracklistRenderer")[1]
            captions_url = captions_url.split("baseUrl\\\":\\\"")[1].split("\\\",\\\"name")[0]
            self.captions_url = self.clean_url(captions_url)
            self.captions_url_response, self.captions_url_status = \
                self.get_response_through_proxy(self.scraper, self.captions_url)
            soup = bs(self.captions_url_response, 'xml')
            captions = replace_apostrophes(
                " ".join([line.strip() for line in soup.find_all(text=True)])) if soup else ""
            captions = re.sub(r"<font.+?>", "", captions)
            captions = re.sub(r"<\/font>", "", captions)
            captions = captions.replace(".", ". ").replace("?", "? ").replace("!", "! ")
            self.captions = captions
            self.captions_language = self.captions_url.split("&lang=")[1].split("&")[0].split("-")[0]
        # pylint: disable=broad-except
        except Exception as e:
            # pylint: enable=broad-except
            self.update_failure_reason(e)

    def get_response_through_proxy(self, scraper, url):
        proxy = scraper.get_proxy()
        headers = scraper.get_headers()
        response = None
        counter = 0
        try:
            # print(f"Sending Request #{counter} to URL: '{url}' through Proxy: '{proxy}'")
            response = requests.get(url=url, proxies=proxy, headers=headers, timeout=scraper.TIMEOUT)
            # print(f"Received Response with Status Code: '{response.status_code}' from Proxy: '{proxy}'")
        except ConnectionError:
            pass
        except Timeout:
            pass
        while (not response or response.status_code != 200) and counter < scraper.NUM_RETRIES:
            counter += 1
            try:
                headers = scraper.get_headers()
                response = requests.get(url=url, proxies=proxy, headers=headers, timeout=scraper.TIMEOUT)
            except ConnectionError:
                continue
            except Timeout:
                continue
        if counter >= scraper.NUM_RETRIES:
            raise Exception("Exceeded connection attempts to URL.")
        response_text = response.text
        response_status = response.status_code
        return response_text, response_status

# pylint: enable=too-many-instance-attributes