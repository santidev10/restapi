"""
SegmentKeyword models module
"""
import logging
from django.contrib.postgres.fields import JSONField
from django.db.models import BigIntegerField
from django.db.models import CharField
from django.db.models import FloatField
from django.db.models import ForeignKey

from aw_reporting.models import KeywordStatistic
from singledb.connector import SingleDatabaseApiConnector as Connector
from .base import BaseSegment
from .base import BaseSegmentRelated
from .base import SegmentManager

logger = logging.getLogger(__name__)


class SegmentKeywordManager(SegmentManager):
    def update_youtube_segments(self):
        query_params = {
            'size': 0,
            'aggregations': 'category',
            'fields': 'video_id',
            'sources': (),
        }
        response = Connector().get_keyword_list(query_params=query_params)
        filters_categories = dict(response['aggregations']['category:count'])
        categories = [k for k, v in filters_categories.items()]
        for category in categories:
            logger.info('Updating youtube keyword segment by category: {}'.format(category))
            query_params = {
                'sort_by': 'views:desc',
                'fields': 'keyword',
                'sources': (),
                'category__terms': category,
                'size': '1000',
            }
            result = Connector().get_keyword_list(query_params=query_params)
            items = result.get('items', [])
            ids = [i['keyword'] for i in items]
            segment, created = self.get_or_create(title=category, category=self.model.CHF)
            segment.replace_related_ids(ids)
            segment.update_statistics(segment)
            logger.info('   ... keywords: {}'.format(len(ids)))


class SegmentKeyword(BaseSegment):
    YOUTUBE = "youtube"
    CHF = "chf"
    BLACKLIST = "blacklist"
    PRIVATE = "private"

    CATEGORIES = (
        (YOUTUBE, YOUTUBE),
        (CHF, CHF),
        (BLACKLIST, BLACKLIST),
        (PRIVATE, PRIVATE),
    )

    category = CharField(max_length=255, choices=CATEGORIES)
    keywords = BigIntegerField(default=0, db_index=True)
    average_volume = BigIntegerField(default=0, db_index=True)
    average_cpc = FloatField(default=0, db_index=True)
    competition = FloatField(default=0, db_index=True)
    top_keywords = JSONField(null=True, blank=True)

    related_aw_statistics_model = KeywordStatistic
    singledb_method = Connector().get_keyword_list

    segment_type = 'keyword'

    objects = SegmentKeywordManager()

    def obtain_singledb_data(self, ids_hash):
        """
        Execute call to SDB
        """
        result = {}
        params = {
            "ids_hash": ids_hash,
            "fields": "keyword",
            "sources": (),
            "size": 0,
            "aggregations": "avg_search_volume,avg_average_cpc,avg_competition",
        }
        result['data'] = self.singledb_method(query_params=params)

        params = {
            "ids_hash": ids_hash,
            "fields": "keyword,search_volume",
            "sources": (),
            "sort": "search_volume:desc",
            "size": 10,
        }
        result['top_keywords'] = self.singledb_method(query_params=params)
        return result

    def populate_statistics_fields(self, data):
        """
        Update segment statistics fields
        """

        self.keywords = data.get('data', {}).get('items_count')

        average_volume = data.get('data', {}).get('aggregations', {}).get('avg_search_volume')
        if average_volume:
            self.average_volume = average_volume[0].get('value')

        average_cpc = data.get('data', {}).get('aggregations', {}).get('avg_average_cpc')
        if average_cpc:
            self.average_cpc = average_cpc[0].get('value')

        competition = data.get('data', {}).get('aggregations', {}).get('avg_competition')
        if competition:
            self.competition = competition[0].get('value')

        keywords = data['top_keywords']['items']
        if keywords:
            self.top_keywords = [{'keyword': kw['keyword'], 'value': kw['search_volume']} for kw in keywords]

    @property
    def statistics(self):
        """
        Count segment statistics
        """
        statistics = {
            "keywords_count": self.keywords,
            "average_volume": self.average_volume,
            "average_cpc": self.average_cpc,
            "competition": self.competition,
            "top_keywords": self.top_keywords,
        }
        return statistics


class SegmentRelatedKeyword(BaseSegmentRelated):
    segment = ForeignKey(SegmentKeyword, related_name='related')
