import re
import csv

from django.conf import settings

from brand_safety.models import BadWord
from singledb.connector import SingleDatabaseApiConnector as Connector


class AuditProvider(object):
    def get_brand_safety_regexp(self):
        if settings.USE_LEGACY_BRAND_SAFETY:
            connector = Connector()
            bad_words = connector.get_bad_words_list({})
            bad_words_names = [item["name"] for item in bad_words]
        else:
            bad_words_names = BadWord.objects.values_list("name", flat=True)
        bad_words_names = list(set(bad_words_names))

        return bad_words_names

    @staticmethod
    def compile_audit_regexp(keywords: list):
        """
        Compiles regular expression with given keywords
        :param keywords: List of keyword strings
        :return: Compiled Regular expression
        """
        regexp = re.compile(
            "({})".format("|".join([r"\b{}\b".format(re.escape(word)) for word in keywords]))
        )
        return regexp

    @staticmethod
    def batch(iterable, length):
        """
        Generator that yields equal sized chunks of iterable
        """
        for i in range(0, len(iterable), length):
            yield iterable[i:i + length]

    @staticmethod
    def read_and_create_keyword_regexp(csv_path):
        """
        Read in csv file for keywords
        :return: Compiled regular expression
        """
        with open(csv_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            keywords = list(csv_reader)

            keyword_regexp = re.compile(
                '|'.join([word[0] for word in keywords]),
                re.IGNORECASE
            )

        return keyword_regexp
