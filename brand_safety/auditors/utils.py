from collections import defaultdict
from collections import namedtuple
import csv
from datetime import datetime
from datetime import timezone
import re

import pytz

from flashtext import KeywordProcessor
from django.db.models import F
from emoji import UNICODE_EMOJI

from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from utils.lang import remove_mentions_hashes_urls
from utils.lang import fasttext_lang


KeywordHit = namedtuple("KeywordHit", "name location")


class AuditUtils(object):
    def __init__(self):
        """
        Many of these configurations are shared amongst Video and Channel audit objects, so it is more efficient to initialize
        these values once and use as reference values
        """
        self.bad_word_categories = BadWordCategory.objects.values_list("id", flat=True)
        # Initial category brand safety scores for videos and channels, since ignoring certain categories (e.g. Kid's Content)
        self._default_zero_score = {
            str(category_id): 0
            for category_id in self.bad_word_categories
        }
        self._default_full_score = {
            str(category_id): 100
            for category_id in self.bad_word_categories
        }
        self._bad_word_processors_by_language = self.get_bad_word_processors_by_language()
        self._emoji_regex = self.compile_emoji_regexp()
        self._score_mapping = self.get_brand_safety_score_mapping()

    @property
    def bad_word_processors_by_language(self):
        return self._bad_word_processors_by_language

    @property
    def emoji_regex(self):
        return self._emoji_regex

    @property
    def default_zero_score(self):
        return self._default_zero_score.copy()

    @property
    def default_full_score(self):
        return self._default_full_score.copy()

    @property
    def score_mapping(self):
        return self._score_mapping

    @staticmethod
    def audit(text, location, keyword_processor):
        """
        Finds all matches of regexp in audit metadata
        :param text: text to parse
        :param location: text location e.g. title, description, etc.
        :param keyword_processor: flashtext module KeywordProcessor instance
        :return:
        """
        hits = [
            KeywordHit(name=hit, location=location)
            for hit in keyword_processor.extract_keywords(text)
        ]
        return hits

    @staticmethod
    def is_working_hours(start=5, end=17):
        """
        Check if current hour is within working hours
        :param start:
        :param end:
        :return:
        """
        pst_tz = pytz.timezone("US/Pacific")
        utc_dt = datetime.now(timezone.utc)
        pst_now_hour = utc_dt.astimezone(pst_tz).hour
        if start <= pst_now_hour <= end:
            return True
        return False

    def update_config(self):
        self._score_mapping = self.get_brand_safety_score_mapping()
        self._bad_word_processors_by_language = self.get_bad_word_processors_by_language()

    def get_brand_safety_regexp(self):
        """
        Get and comple brand safety tags
        :return:
        """
        bad_words_names = BadWord.objects.values_list("name", flat=True)
        bad_words_names = list(set(bad_words_names))
        brand_safety_regexp = self.compile_regexp(bad_words_names)

        return brand_safety_regexp

    def has_emoji(self, text):
        has_emoji = bool(re.search(self._emoji_regex, text))
        return has_emoji

    @staticmethod
    def get_trie_keyword_processor(words):
        keyword_processor = KeywordProcessor()
        for word in words:
            keyword_processor.add_keyword(word)
        return keyword_processor

    @staticmethod
    def increment_cursor(script_tracker, value):
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

    @staticmethod
    def get_bad_word_processors_by_language():
        """
        Generate dictionary of keyword processors by language
            Also provides an "all" key that contains every keyword
        :return:
        """
        bad_words_by_language = defaultdict(KeywordProcessor)
        all_words = BadWord.objects.annotate(language_name=F("language__language"))
        for word in all_words:
            language = word.language_name
            bad_words_by_language["all"].add_keyword(word.name)
            bad_words_by_language[language].add_keyword(word.name)
        # Cast back to dictionary to avoid creation of new keys
        bad_words_by_language = dict(bad_words_by_language)
        return bad_words_by_language

    @staticmethod
    def get_brand_safety_score_mapping():
        """
        Map brand safety BadWord rows to their score
        :return: dict
        """
        score_mapping = defaultdict(dict)
        for word in BadWord.objects.all():
            score_mapping[word.name] = {
                "category": str(word.category_id),
                "score": word.negative_score
            }
        return score_mapping

    @staticmethod
    def get_bad_words():
        """
        Get brand safety words
            Kid's content brand safety words are not included in brand safety score calculations
        :return:
        """
        bad_words = BadWord.objects \
            .values_list("name", flat=True)
        return bad_words

    @staticmethod
    def get_language(text):
        """
        Analyzes metadata for language using fastText module
        :param text: text to analyze
        :return: Language code
        """
        text = " ".join(text.split("\n"))
        text = remove_mentions_hashes_urls(text)
        language = fasttext_lang(text)
        return language

    @staticmethod
    def get_items(item_ids, manager=None):
        """
        Retrieve documents
        :param item_ids: list -> Video or channel ids
        :param manager: Elasticsearch manager
        :return: list -> Elasticsearch documents
        """
        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value(item_ids).get()
        results = manager.search(query).execute().hits
        return results

    @staticmethod
    def reset_brand_safety_scores(item_ids, model=None, manager=None):
        """
        Reset brand safety score
        :param item_ids: list -> Channel or video ids
        :param manager: ES Manager instantiated with upsert: brand safety section
        :return:
        """
        if type(item_ids) is str:
            item_ids = [item_ids]
        if Sections.BRAND_SAFETY not in manager.upsert_sections:
            raise ValueError(f"Manager must include section: f{Sections.BRAND_SAFETY} in upsert section.")
        updated = [
            model(**{
                "meta": {"id": _id},
                "brand_safety": {
                    "overall_score": None,
                    "videos_scored": None,
                    "language": None,
                    "categories": None
                }
            }) for _id in item_ids
        ]
        manager.upsert(updated)
