import re
from datetime import datetime
from datetime import timedelta
from itertools import zip_longest

from rest_framework.fields import SerializerMethodField

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
        items_count = 0
        history = zip_longest(
            reversed(video.stats.views_history or []),
            reversed(video.stats.likes_history or []),
            reversed(video.stats.dislikes_history or []),
            reversed(video.stats.comments_history or [])
        )
        for views, likes, dislikes, comments in history:
            timestamp = video.stats.historydate - timedelta(
                days=len(video.stats.views_history) - items_count - 1)
            timestamp = datetime.combine(timestamp, datetime.max.time())
            items_count += 1
            if any((views, likes, dislikes, comments)):
                chart_data.append(
                    {"created_at": "{}{}".format(str(timestamp), "Z"),
                     "views": views,
                     "likes": likes,
                     "dislikes": dislikes,
                     "comments": comments}
                )
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
