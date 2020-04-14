import re
from datetime import datetime
from datetime import timedelta
from itertools import zip_longest

from rest_framework.fields import SerializerMethodField

from utils.datetime import date_to_chart_data_str
from utils.brand_safety import get_brand_safety_data
from utils.es_components_api_utils import ESDictSerializer
from brand_safety.languages import LANG_CODES


class VideoSerializer(ESDictSerializer):
    chart_data = SerializerMethodField()
    transcript = SerializerMethodField()
    brand_safety_data = SerializerMethodField()

    def get_chart_data(self, video):
        if not video.stats:
            return []

        chart_data = []
        views_raw_history = video.stats.views_raw_history.to_dict()
        likes_raw_history = video.stats.likes_raw_history.to_dict()
        dislikes_raw_history = video.stats.dislikes_raw_history.to_dict()
        comments_raw_history = video.stats.comments_raw_history.to_dict()

        history_dates = set(list(views_raw_history.keys()) + list(likes_raw_history.keys()) +
                            list(dislikes_raw_history.keys()) + list(comments_raw_history.keys()))

        for history_date in sorted(list(history_dates)):
            chart_data.append({
                "created_at": date_to_chart_data_str(history_date),
                "views": views_raw_history.get(history_date),
                "likes": likes_raw_history.get(history_date),
                "dislikes": dislikes_raw_history.get(history_date),
                "comments": comments_raw_history.get(history_date)
            })

        return chart_data

    def get_transcript(self, video):
        text = ""
        try:
            vid_language = video.general_data.language
            vid_lang_code = LANG_CODES[vid_language.capitalize()]
        except Exception as e:
            vid_lang_code = 'en'

        if 'captions' in video and 'items' in video.captions:
            for item in video.captions.items:
                if item.language_code == vid_lang_code:
                    text = item.text
                    break
            if not text:
                for item in video.captions.items:
                    if item.language_code == "en":
                        text = item.text
                        break
            if not text:
                text = video.captions.items[0].text
        if not text and 'custom_captions' in video and 'items' in video.custom_captions:
            for item in video.custom_captions.items:
                if item.language_code == vid_lang_code:
                    text = item.text
                    break
            if not text:
                for item in video.captions.items:
                    if item.language_code == "en":
                        text = item.text
                        break
            if not text:
                text = video.custom_captions.items[0].text
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
        return transcript

    def get_brand_safety_data(self, channel):
        return get_brand_safety_data(channel.brand_safety.overall_score)


REGEX_TO_REMOVE_TIMEMARKS = "^\s*$|((\r\n|\n|\r|\,|)(\d+(\:\d+\:\d+[.,]\d+|))(\s+-->\s+\d+\:\d+\:\d+[.,]\d+|))"
