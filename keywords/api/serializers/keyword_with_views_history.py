from rest_framework.fields import SerializerMethodField

from keywords.api.views.utils import get_views_keyword_history_chart
from utils.es_components_api_utils import ESDictSerializer


class KeywordWithViewsHistorySerializer(ESDictSerializer):
    views_history_chart = SerializerMethodField()

    def get_views_history_chart(self, item):
        return get_views_keyword_history_chart(item)

    def create(self, validated_data):
        raise NotImplementedError

    def update(self, instance, validated_data):
        raise NotImplementedError
