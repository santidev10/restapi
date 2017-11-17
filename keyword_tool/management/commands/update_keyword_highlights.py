import logging
from itertools import groupby as g

from django.core.management.base import BaseCommand

from keyword_tool.models import KeyWord
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    all_keywords = KeyWord.objects.all()[:100]
    update_params = ('category', 'thirty_days_views', 'weekly_views', 'daily_views', 'views')
    connector = SingleDatabaseApiConnector()

    def handle(self, *args, **options):
        keywords_count = self.all_keywords.count()
        logger.info("{} keywords will be updated".format(keywords_count))
        for keyword in self.all_keywords:
            for related_video_data in self.gen_related_video_data(keyword):
                self.update_keyword(keyword, related_video_data)
                logger.info(">{}< keyword updated".format(keyword.text))
                keywords_count -= 1
                logger.info("{} keywords remaining".format(keywords_count))

    def update_keyword(self, keyword, keyword_details):

        def most_common_category(L):
            if L:
                return max(g(sorted(L)), key=lambda p: (lambda x, y: (len(list(y)), -L.index(x)))(*p))[0]

        for param in self.update_params:
            if param == 'category':
                setattr(keyword, param, most_common_category([item['category'] for item in keyword_details]))
                continue

            setattr(keyword, param, sum(item[param] for item in keyword_details if item[param] is not None))

        keyword.save()

    def gen_related_video_data(self, keyword):
        query_params = {}
        query_params["keywords__term"] = keyword.text
        query_params["fields"] = ','.join(self.update_params)
        query_params["size"] = 500
        query_params["page"] = 1
        yield from self.gen_video_from(query_params)

    def gen_video_from(self, query_params):
        result = []

        while True:
            try:
                response_data = self.connector.get_video_list(query_params=query_params)
            except SingleDatabaseApiConnectorException:
                break

            if not response_data.get('items'):
                break

            result += response_data['items']

            if not response_data['current_page'] == response_data['max_page']:
                query_params['page'] += 1
                continue

            break

        yield result
