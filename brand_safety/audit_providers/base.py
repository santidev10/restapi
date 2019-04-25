import re
import csv

from flashtext import KeywordProcessor
from django.conf import settings
from emoji import UNICODE_EMOJI

from brand_safety.models import BadWord
from singledb.connector import SingleDatabaseApiConnector as Connector


class AuditProvider(object):
    def __init__(self):
        self.emoji_regexp = self.compile_emoji_regexp()

    def get_brand_safety_regexp(self):
        """
        Get and comple brand safety tags
        :return:
        """
        if settings.USE_LEGACY_BRAND_SAFETY:
            connector = Connector()
            bad_words = connector.get_bad_words_list({})
            bad_words_names = [item["name"] for item in bad_words]
        else:
            bad_words_names = BadWord.objects.values_list("name", flat=True)
        bad_words_names = list(set(bad_words_names))
        brand_safety_regexp = self.compile_regexp(bad_words_names)

        return brand_safety_regexp

    @staticmethod
    def get_trie_keyword_processor(words):
        keyword_processor = KeywordProcessor()
        for word in words:
            keyword_processor.add_keyword(word)
        return keyword_processor

    @staticmethod
    def update_cursor(script_tracker, value):
        """
        Update APIScriptTracker instance cursor value
        :param script_tracker: APIScriptTracker instance
        :param value: Updated cursor value
        :return: APIScriptTracker instance
        """
        try:
            int(value)
        except ValueError:
            raise ValueError("This method should only be used to update cursor with integer values.")
        script_tracker.cursor += value
        script_tracker.save()
        return script_tracker

    @staticmethod
    def set_cursor(script_tracker, value, integer=True):
        """
        Set APIScriptTracker instance cursor
        :param script_tracker: APIScriptTracker instance
        :param value: Cursor value to set
        :param integer: Flag to set tracker integer field or char field
        :return: script_tracker
        """
        if integer:
            script_tracker.cursor = value
        else:
            script_tracker.cursor_id = value
        script_tracker.save()
        return script_tracker

    @staticmethod
    def compile_regexp(keywords: list, case_insensitive=True):
        """
        Compiles regular expression with given keywords
        :param keywords: List of keyword strings
        :return: Compiled Regular expression
        """
        regexp = re.compile("({})".format("|".join([r"\b{}\b".format(re.escape(word)) for word in keywords]), re.IGNORECASE)) \
            if case_insensitive else re.compile("({})".format("|".join([r"\b{}\b".format(re.escape(word)) for word in keywords])))
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

    @staticmethod
    def compile_emoji_regexp():
        regexp = re.compile(
            "({})".format("|".join([r"{}".format(re.escape(unicode)) for unicode in UNICODE_EMOJI]))
        )
        return regexp
