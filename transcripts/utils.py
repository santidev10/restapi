import requests
import urllib.parse as urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup as bs
from django.core.exceptions import ValidationError
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY


class YTVideo(object):
    YT_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.7",
        "User-Agent": "Mozilla/5.0 (compatible; Google2SRT/0.7.8)"
    }
    NUM_SUBTITLES_TO_PULL = 5

    def __init__(self, vid_id):
        self.vid_id = vid_id
        self.vid_url = self.get_vid_url(vid_id)
        self.vid_url_response = self.get_vid_url_response(self.vid_url)
        self.vid_url_status = self.vid_url_response.status_code
        self.tts_url = self.get_tts_url(self.vid_url_response)
        self.params = self.parse_tts_url_params(self.tts_url)
        self.subtitles_list_url = self.get_list_url(self.params)
        self.subtitles_list_url_response = self.get_list_url_response(self.subtitles_list_url)
        self.tracks_meta, self.targets_meta = self.parse_list_url(self.subtitles_list_url_response)
        self.asr_track_meta = self.get_asr_track(self.tracks_meta)
        self.asr_lang_code = self.asr_track_meta.get("lang_code")
        self.tracks_lang_codes_dict = self.get_lang_codes_dict(self.tracks_meta)
        self.targets_lang_codes_dict = self.get_lang_codes_dict(self.targets_meta)
        self.top_lang_codes, self.top_subtitles_meta = self.get_top_subtitles_meta()
        self.subtitles = self.get_top_subtitles()

    def get_top_subtitles(self):
        if not self.top_subtitles_meta:
            return None
        top_subtitles = []
        for subtitle_meta in self.top_subtitles_meta:
            subtitle_options = {
                "vid_id": self.vid_id,
                "params": self.params,
                "subtitle_meta": subtitle_meta
            }
            if self.asr_track_meta:
                subtitle_options["asr_track_meta"] = self.asr_track_meta
            subtitle = YTVideoSubtitles(**subtitle_options)
            top_subtitles.append(subtitle)
        return top_subtitles

    def get_top_subtitles_meta(self):
        best_lang_codes = []
        best_lang_codes_meta = []
        available_lang_codes = set()
        available_lang_codes.update(self.tracks_lang_codes_dict)
        available_lang_codes.update(self.targets_lang_codes_dict)
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
        lang_codes_meta.append(self.tracks_lang_codes_dict.get(lang_code) or
                               self.targets_lang_codes_dict.get(lang_code))
        return lang_codes, lang_codes_meta

    @staticmethod
    def get_vid_url(vid_id: str):
        return f"https://www.youtube.com/watch?v={vid_id}"

    def get_vid_url_response(self, vid_url):
        return requests.get(vid_url, headers=self.YT_HEADERS)

    @staticmethod
    def get_tts_url(yt_response):
        if yt_response.status_code != 200:
            return None
        else:
            yt_response_html = yt_response.text
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

    def get_list_url_response(self, list_url):
        if not list_url:
            return None
        return requests.get(list_url, headers=self.YT_HEADERS)

    def parse_list_url(self, list_url_response):
        if not list_url_response:
            return [], []
        soup = bs(list_url_response.text, 'xml')
        transcript_list = soup.transcript_list
        docid = transcript_list.attrs.get('docid')
        if not docid:
            raise ValidationError("list_url has no 'docid' attribute.")
        tracks = self.get_tracks(soup)
        targets = self.get_targets(soup)
        num_tracks = len(tracks) + len(targets)
        if num_tracks < 1:
            raise ValidationError("list_url has no tracks.")
        return tracks, targets

    @staticmethod
    def get_tracks(soup: bs):
        return [track for track in soup.find_all("track")]

    @staticmethod
    def get_targets(soup: bs):
        return [target for target in soup.find_all("target")]

    @staticmethod
    def get_asr_track(tracks):
        asr_track = [track for track in tracks if track.get("kind") == "asr"]
        return asr_track[0] if asr_track else {}

    @staticmethod
    def get_lang_codes_dict(items):
        return {item.get("lang_code"): item for item in items if item.get("lang_code")}




class YTVideoSubtitles(object):
    def __init__(self, vid_id, params=None, subtitle_meta=None, asr_track_meta=None):
        self.vid_id = vid_id
        self.params = params
        self.subtitle_meta = subtitle_meta
        self.asr_track_meta = asr_track_meta
        self.subtitle_id = None
        self.name = None
        self.is_asr = None
        self.asr_lang_code = None
        self.asr_lang_original = None
        self.asr_lang_translated = None
        self.lang_code = None
        self.lang_original = None
        self.lang_translated = None
        self.type = None
        self.subtitle_url = None
        self.set_meta_data()

    def set_meta_data(self):
        self.subtitle_id = self.subtitle_meta.get('id')
        self.name = self.asr_track_meta.get('name')
        self.is_asr = True if self.subtitle_meta.get('kind') == 'asr' else False
        self.asr_lang_code = self.asr_track_meta.get('lang_code')
        self.asr_lang_original = self.asr_track_meta.get('lang_original')
        self.asr_lang_translated = self.asr_track_meta.get('lang_translated')
        self.lang_code = self.subtitle_meta.get('lang_code')
        self.lang_original = self.subtitle_meta.get('lang_original')
        self.lang_translated = self.subtitle_meta.get('lang_translated')
        self.type = self.subtitle_meta.name
        self.subtitle_url = self.get_subtitle_url()

    def is_track(self):
        return self.type == "track"

    def is_target(self):
        return self.type == "target"

    def get_subtitle_url(self):
        subtitle_url = "https://www.youtube.com/api/timedtext?"
        for key, value in self.params.items():
            subtitle_url += f"{key}={value}&"
        subtitle_url += f"name={self.name}&lang={self.asr_lang_code}&type=track"
        if self.asr_track_meta:
            subtitle_url += "&kind=asr"
        if self.is_target():
            subtitle_url += f"&tlang={self.lang_code}"
        return subtitle_url


class YTTranscriptsScraper(object):
    BATCH_SIZE = 100

    def __init__(self, vid_ids):
        self.vid_ids = vid_ids
        self.vids = []
        self.num_failed_vids = None
        self.failure_reasons = None

    def retrieve_transcripts(self):
        failed_vid_reasons = {}
        for vid_id in self.vid_ids:
            try:
                yt_vid = YTVideo(vid_id)
                self.vids.append(yt_vid)
                if yt_vid.vid_url_status != 200:
                    raise Exception(f"Failed to get response from Youtube for Video: '{vid_id}'. "
                                    f"Received status code: '{yt_vid.vid_url_status}' from URL '{yt_vid.vid_url}'")
                elif not yt_vid.tts_url:
                    raise Exception(f"No TTS_URL for Video: '{vid_id}'.")
                elif not yt_vid.subtitles_list_url:
                    raise Exception(f"No TRACKS_LIST_URL found for Video: '{vid_id}'.")
                elif not yt_vid.tracks_meta and not yt_vid.targets_meta:
                    raise Exception(f"Video: '{vid_id}' has no TTS_URL captions available.")
            except Exception as e:
                failed_vid_reasons[vid_id] = e
                continue
        if len(failed_vid_reasons) > 0:
            self.num_failed_vids = len(failed_vid_reasons)
            self.failure_reasons = failed_vid_reasons
