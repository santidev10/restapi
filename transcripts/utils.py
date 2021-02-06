import re
import requests
from html import unescape
from threading import Thread

from bs4 import BeautifulSoup
from celery.exceptions import Retry
from datetime import timedelta
from django.conf import settings
from django.utils import timezone
from requests.exceptions import ConnectionError, Timeout

from administration.notifications import send_email
from utils.celery.tasks import lock
from utils.lang import replace_apostrophes


def get_formatted_captions_from_soup(soup: BeautifulSoup) -> str:
    """
    Takes a soup and formats outgoing captions from it
    :param soup:
    :return: str
    """
    if not isinstance(soup, BeautifulSoup):
        return ""
    lines = soup.find_all(text=True)
    captions = " ".join([line.strip() for line in lines])
    captions = unescape(captions)
    captions = replace_apostrophes(captions)
    captions = re.sub(r"<font.+?>", "", captions)
    captions = re.sub(r"<\/font>", "", captions)
    # replace any number of periods, question marks, exclamation marks, or double quotes (group 1)
    # followed by one or more digits or alphas (group 2)
    # with the first and second group bisected by a space.
    # This upgrades the simple period, exclaim, question space appending with something a tad smarter
    captions = re.sub(r"([\.\?\!\"]+)([a-zA-Z0-9]+)", r"\1 \2", captions)
    return captions


class YTTranscriptsScraper(object):
    """ Utility Class for scraping ASR Transcripts"""
    EMAILER_LOCK_NAME = "transcripts_alert_emailer"
    NUM_RETRIES = 3
    NUM_THREADS = settings.TRANSCRIPTS_NUM_THREADS
    TIMEOUT = settings.TRANSCRIPTS_TIMEOUT
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
        """ This method will run the entire scraper and gather the results. """
        self.retrieve_transcripts()
        self.gather_success_and_failures()

    def create_yt_vids(self):
        """
        This method is called when scraper is instantiated. It creates YTVideo object for all video IDs in self.vid_ids
        and stores them in self.vids.
        """
        for vid_id in self.vid_ids:
            yt_vid = YTVideo(vid_id, self)
            self.vids.append(yt_vid)

    def retrieve_transcripts(self):
        """ Multithreaded method for Retrieving ASR captions for all YTVideo objects in self.vids """
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
        """
        After retrieve_transcripts is called, this method gathers the results for all the YTVideo objects generated by
        the scraper and updates self.successful_vids, self.failure_reasons, and self.num_failed_vids with the
        appropriate values.
        """
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
        """
        Using self.host and self.port, this method generates dictionary to pass in as the value for the proxies
        parameter when making requests with the requests module.
        """
        return {
            "http": f"http://{self.host}:{self.port}",
            "https": f"http://{self.host}:{self.port}"
        }

    def get_user_agent(self):
        """
        This method was used for User Agent switching, but the User Agent switching package we used before broke, so now
        it's just using a static user agent (the one used by Gthe oogle2SRT app). The scraper seems to run fine without
        user agent switching for now, but we may need to re-implement user agent switching in the future, so I left this
        method in for documentation purposes.
        """
        return 'Mozilla/5.0 (compatible; Google2SRT/0.7.8)'

    def get_headers(self):
        """
        Generates the headers for requests. Mainly used for switching user agent. For more info, read docs for
        'get_user_agent' method.
        """
        headers = self.YT_HEADERS
        headers['User-Agent'] = self.get_user_agent()
        return headers

    def send_yt_blocked_email(self):
        """ If all proxies have been blocked, this method locks scraper for an hour and sends an email notification. """
        try:
            lock(lock_name=self.EMAILER_LOCK_NAME, max_retries=1, expire=timedelta(minutes=60).total_seconds())
        except Retry:
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
    """ Utility class for storing the metadata from scraping each individual Youtube Video"""
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
        """ Only update failure_reason if it does not already have a value. """
        if not self.failure_reason:
            self.failure_reason = e

    @staticmethod
    def get_vid_url(vid_id: str):
        """ Generate YT URL for Video ID"""
        return f"http://www.youtube.com/watch?v={vid_id}"

    @staticmethod
    def clean_url(url):
        """ Cleans improperly formatted URLs before making requests to them."""
        url = url.replace("\\", "")
        url = url.replace("u0026", "&")
        return url

    @staticmethod
    def get_raw_captions_url(vid_response: str) -> str:
        """
        Gets the raw, uncleaned captions url from a given video response string
        verbose for ez debugging
        NOTE: If Youtube ever changes the internal API URL it uses for retrieving ASR captions, this method will need
        to be updated.
        :param vid_response:
        :return captions_url: str
        """
        split = vid_response.split("\"captions\"")
        captions_url = split[1]
        split = captions_url.split("\"playerCaptionsTracklistRenderer\"")
        captions_url = split[1]
        split = captions_url.split("\"baseUrl\"")
        captions_url = split[1]
        split = captions_url.split("\"")
        captions_url = split[1]
        return captions_url

    @staticmethod
    def get_captions_language(captions_url: str) -> str:
        """
        get the captions language from the given captions url
        verbose for easy debugging
        :param captions_url:
        :return language:
        """
        split = captions_url.split("&lang=")
        language = split[1]
        split = language.split("&")
        language = split[0]
        split = language.split("-")
        language = split[0]
        return language

    def get_captions(self):
        """
        Parses the response from a YT Video's URL to find the internal API URL Youtube is using to generate ASR captions,
        then makes a request to that internal API URL to retrieve the video's ASR captions. Then cleans those captions
        and stores the captions and language.

        NOTE: If Youtube ever changes the internal API URL it uses for retrieving ASR captions, this method will need
        to be updated.
        """
        try:
            vid_response, self.vid_url_status = self.get_response_through_proxy(self.scraper, self.vid_url)
            print(self.vid_url_status)
            raw_captions_url = self.get_raw_captions_url(vid_response)
            self.captions_url = self.clean_url(raw_captions_url)
            self.captions_url_response, self.captions_url_status = \
                self.get_response_through_proxy(self.scraper, self.captions_url)
            soup = BeautifulSoup(self.captions_url_response, 'xml')
            captions = get_formatted_captions_from_soup(soup)
            self.captions = captions
            self.captions_language = self.get_captions_language(self.captions_url)
        # pylint: disable=broad-except
        except Exception as e:
            # pylint: enable=broad-except
            self.update_failure_reason(e)

    def get_response_through_proxy(self, scraper, url):
        """
        All scraper requests should be made through this method. It will handle all the proxy switching edge cases.
        """
        proxy = scraper.get_proxy()
        headers = scraper.get_headers()
        response = None
        counter = 0
        try:
            response = requests.get(url=url, proxies=proxy, headers=headers, timeout=scraper.TIMEOUT)
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
