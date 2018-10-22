import json
import logging
import os
import time

import suds
from django.core.management.base import BaseCommand
from django.db import transaction

from aw_reporting.adwords_api import optimize_keyword, get_client
from keyword_tool.models import KeyWord, Interest, KeywordsList, AVAILABLE_KEYWORD_LIST_CATEGORIES
from keyword_tool.tasks import update_keywords_stats, update_kw_list_stats

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.join(BASE_DIR, '../..')

CF_CATEGORY = AVAILABLE_KEYWORD_LIST_CATEGORIES[2]


class Command(BaseCommand):
    client = get_client()
    file_name = 'vertical_keywords.csv'

    def handle(self, *args, **options):
        for line in self.get_search_data():
            response = []
            category, *keywords = line.split(',')
            for chunk in self.chunks(list(set([k.lower() for k in keywords])), 200):
                response += self.get_awd_response(chunk)
            created, updated = self.create_or_update_keywords(response)
            kw_list = list(set([str(k.text) for k in created] + [kw['keyword_text'] for kw in updated]))
            self.create_or_update_kw_lists(email='admin@admin.admin',
                                           name=category,
                                           category=CF_CATEGORY,
                                           keywords=kw_list)

    def create_or_update_keywords(self, response):
        # models
        interest_relation = KeyWord.interests.through

        # get ids
        interest_ids = set(
            Interest.objects.all().values_list('id', flat=True)
        )
        keywords_ids = set(
            KeyWord.objects.filter(
                text__in=[i['keyword_text'] for i in response]
            ).values_list('text', flat=True)
        )

        # create items
        kws = []
        update_kws = []
        interest_relations = []
        for k in response:
            keyword_text = k['keyword_text']
            if keyword_text in keywords_ids:
                update_kws.append(k)
            else:
                kws.append(
                    KeyWord(
                        text=keyword_text,
                        average_cpc=k.get('average_cpc'),
                        competition=k.get('competition'),
                        _monthly_searches=json.dumps(
                            k.get('monthly_searches', [])
                        ),
                        search_volume=k.get('search_volume'),
                    )
                )
                for interest_id in k['interests']:
                    if interest_id in interest_ids:
                        interest_relations.append(
                            interest_relation(
                                keyword_id=keyword_text,
                                interest_id=interest_id,
                            )
                        )

        with transaction.atomic():
            if kws:
                KeyWord.objects.safe_bulk_create(kws)
            if interest_relations:
                interest_relation.objects.bulk_create(interest_relations)

        if update_kws:
            update_keywords_stats.delay(update_kws)

        return kws, update_kws

    def create_or_update_kw_lists(self, email, name, category, keywords):
        kw_list, new = KeywordsList.objects.get_or_create(
            user_email=email,
            name=name,
            category=category
        )
        keywords_relation = KeywordsList.keywords.through
        if new:
            kw_relations = [keywords_relation(keyword_id=kw_id,
                                              keywordslist_id=kw_list.id)
                            for kw_id in keywords]
            keywords_relation.objects.bulk_create(kw_relations)

        if not new:
            queryset = KeyWord.objects.filter(pk__in=keywords) \
                .exclude(lists__id=kw_list.id)
            ids_to_save = set(queryset.values_list('text', flat=True))

            if ids_to_save:
                kw_relations = [keywords_relation(keyword_id=kw_id,
                                                  keywordslist_id=kw_list.id)
                                for kw_id in ids_to_save]
                keywords_relation.objects.bulk_create(kw_relations)

        update_kw_list_stats.delay(kw_list, KeyWord)

    def get_search_data(self):
        file_path = os.path.join(BASE_DIR, self.file_name)
        fh = open(file_path, 'r', encoding='utf-8')
        for line in fh:
            yield line.strip()

    def chunks(self, list_do_divide, chunk_size):
        for item in range(0, len(list_do_divide), chunk_size):
            yield list_do_divide[item:item + chunk_size]

    def get_awd_response(self, query):
        while True:
            try:
                return optimize_keyword(query=query,
                                        client=self.client,
                                        request_type='STATS')
            except suds.WebFault:
                logger.info("Sleeping RateExceededError")
                time.sleep(31)
                continue
