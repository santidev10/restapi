import re
from datetime import datetime
from datetime import timedelta
from itertools import zip_longest

from rest_framework.fields import SerializerMethodField

from utils.brand_safety_view_decorator import get_brand_safety_data
from utils.es_components_api_utils import ESDictSerializer


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
        transcript = None
        if video.captions and video.captions.items:
            for caption in video.captions.items:
                if caption.language_code == "en":
                    text = caption.text
                    transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
        return transcript

    def get_brand_safety_data(self, channel):
        return get_brand_safety_data(channel.brand_safety)


REGEX_TO_REMOVE_TIMEMARKS = "^\s*$|((\n|\,|)\d+\:\d+\:\d+\.\d+)"
