from datetime import datetime
from datetime import timedelta
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField

from singledb.models import Channel
from singledb.models import Country
from singledb.models import Video


class ChannelListSerializer(ModelSerializer):
    class Meta:
        model = Channel
        fields = (
            "id",
            "title",
            "thumbnail_image_url",
            "subscribers",
            "thirty_days_subscribers",
            "thirty_days_views",
            "views_per_video",
            "engage_rate",
            "sentiment",
            "country",
            "category",
            "language"
        )

class ChannelDetailsSerializer(ModelSerializer):
    chart_data = SerializerMethodField()

    class Meta:
        model = Channel
        fields = (
            "id",
            "title",
            "thumbnail_image_url",
            "subscribers",
            "thirty_days_subscribers",
            "thirty_days_views",
            "views_per_video",
            "engage_rate",
            "sentiment",
            "country",
            "category",
            "language",

            "description",
            "tags",
            "social_stats",
            "youtube_keywords",

            "chart_data",
        )

    def get_chart_data(self, instance):
        items = []
        items_count = 0
        details = instance.details
        if details:
            history = zip(reversed(details.subscribers_history),
                          reversed(details.sentiment_history),
                          reversed(details.engage_rate_history))
            for subscribers, sentiment, engage_rate in history:
                timestamp = details.history_date - timedelta(days=len(details.subscribers_history)-items_count-1)
                timestamp = datetime.combine(timestamp, datetime.max.time())
                items_count += 1
                if any((subscribers, sentiment, engage_rate)):
                    items.append({'created_at': str(timestamp) + 'Z',
                                  'subscribers': subscribers,
                                  'sentiment': sentiment,
                                  'engage_rate': engage_rate})
        return items


class CountrySerializer(ModelSerializer):
    class Meta:
        model = Country
        fields = (
            "common",
        )


class VideoChannelSerializer(ModelSerializer):
    class Meta:
        model = Channel
        #TODO: review fields list
        fields = (
            "id",
            "title",
            "description",
            "tags",
            "youtube_keywords",
            "emails",
            "thumbnail_image_url",
            "youtube_published_at",
            "updated_at",
            "country",
            "category",
            "social_stats",
            "social_links",
            "is_monetizable",
            "monetization",
            "content_owner",
            "details",
            "from_ad_words",
            "preferred",
            "ptk_value",
            "is_content_safe",
            "subscribers",
            "thirty_days_subscribers",
            "thirty_days_views",
            "views_per_video",
            "sentiment",
            "engage_rate",
            "social_stats",
        )


class VideoListSerializer(ModelSerializer):
    chart_data = SerializerMethodField()

    class Meta:
        model = Video
        fields = (
            "id",
            "title",
            "views",
            "likes",
            "dislikes",
            "comments",
            "sentiment",
            "engage_rate",
            "thumbnail_image_url",
            "country",
            "category",
            "language",

            "chart_data",
        )

    def get_chart_data(self, instance):
        items = []
        items_count = 0
        details = instance.details
        if details:
            history = list(details.views_history[:10])
            for views in history:
                timestamp = details.history_date - timedelta(days=items_count)
                timestamp = datetime.combine(timestamp, datetime.max.time())
                items_count += 1
                items.append({'created_at': str(timestamp) + 'Z', 'views': views})
        return items



class VideoDetailsSerializer(ModelSerializer):
    chart_data = SerializerMethodField()
    channel = VideoChannelSerializer()

    class Meta:
        model = Video
        fields = (
            "id",
            "title",
            "channel",
            "views",
            "likes",
            "dislikes",
            "comments",
            "sentiment",
            "engage_rate",
            "youtube_published_at",
            "thumbnail_image_url",
            "description",
            "country",
            "category",
            "tags",

            "chart_data",
        )

    def get_chart_data(self, instance):
        items = []
        items_count = 0
        details = instance.details
        if details:
            history = zip(reversed(details.views_history),
                          reversed(details.likes_history),
                          reversed(details.dislikes_history),
                          reversed(details.comments_history))
            for views, likes, dislikes, comments in history:
                timestamp = details.history_date - timedelta(days=len(details.views_history)-items_count-1)
                timestamp = datetime.combine(timestamp, datetime.max.time())
                items_count += 1
                if any((views, likes, dislikes, comments)):
                    items.append({'created_at': str(timestamp) + 'Z',
                                  'views': views,
                                  'likes': likes,
                                  'dislikes': dislikes,
                                  'comments': comments})
        return items
