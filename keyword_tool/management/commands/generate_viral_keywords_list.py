import logging
import time

from django.core.management.base import BaseCommand
from django.db import transaction

from aw_reporting.adwords_api import optimize_keyword, get_client
from keyword_tool.models import Query, KeyWord, ViralKeywords
# pylint: disable=import-error
from singledb.connector import SingleDatabaseApiConnector as Connector
# pylint: enable=import-error

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    top_keywords = Connector().get_top_channel_keywords(query_params={})

    def handle(self, *args, **options):
        with transaction.atomic():
            # Delete old keywords
            ViralKeywords.objects.all().delete()

            # fill new keywords to table
            client = get_client()
            for query in self.top_keywords:
                try:
                    Query.objects.get(pk=query)
                except Query.DoesNotExist:
                    try:
                        Query.create_from_aw_response(
                            query,
                            optimize_keyword([q.strip() for q in query.split(',')
                                              if q.strip()], client=client),
                        )
                    except Exception as e:
                        # timeout if too many requests
                        try:
                            inner_error = e.fault.detail.ApiExceptionFault.errors
                            if inner_error.reason == 'RATE_EXCEEDED':
                                logger.info('{}, timeout 30 sec'.format(inner_error.reason))
                                time.sleep(int(inner_error.retryAfterSeconds))
                        except AttributeError:
                            logger.info(e)

            # get all viral keywords
            keywords = {i for i in KeyWord.objects.filter(search_volume__gte=10000) if
                        i.monthly_searches[-1]['value'] > i.monthly_searches[-2]['value'] +
                        i.monthly_searches[-2]['value'] * 0.5}

            # create relations
            kv_to_save = [ViralKeywords(keyword=keyword) for keyword in keywords]
            ViralKeywords.objects.bulk_create(kv_to_save)
