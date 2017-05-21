"""
Mini dash generator module
"""
import heapq
import re
from datetime import timedelta, datetime
from itertools import zip_longest
from string import punctuation

import nltk
from django.utils import timezone
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer

# We had to temporarily limit text length in keywords
#  generating procedure to not drop performance
TEXT_LIMIT_LENGTH = 5000


def get_diff_history_by_period(history_data, days=30):
    """
    :param history_data: list of history values
    :param days: period in days
    :return: list
    """
    history = []
    for i in range(len(history_data)):
        curr_value = history_data[i]
        window = history_data[i:i + days]
        prev_value = window[-1]
        history.append(curr_value - prev_value)
    return history


def get_stopwords_list(language):
    """
    Prepare stop-words list for appropriate language or english by default
    :param language: language
    :return: list of words
    """
    available_languages = stopwords.fileids()
    if (language is None) or (language not in available_languages):
        language = "english"
    stop_words = stopwords.words(language)
    return stop_words


def parse_keywords(stop_words_list, text):
    """
    Extract keywords from text
    :param stop_words_list: words to be ignored
    :param text: string
    :return: list
    """
    words = []
    text = re.sub(r'https?://\S*', '', text)
    tokens = RegexpTokenizer('\w+').tokenize(text)
    tags = nltk.pos_tag(tokens, tagset='universal')
    for word, pos in tags:
        if pos == "NOUN":
            word = word.lower()
            if ((not word.isnumeric()) and (word not in stop_words_list) and
                    (word not in punctuation) and (len(word) > 1)):
                words.append(word)
    return words


def prepare_date_range(start, delta_days):
    """
    Prepare date range starts from current date
    """
    if start is None:
        start = timezone.now().date()
    date_range = [
        (start - timedelta(days=x)).strftime("%Y-%m-%d")
        for x in range(0, delta_days)]
    return date_range


def get_top_keywords_from_text(text, languages):
    """
    Prepare top 30 keywords with rate from text
    :param text: string
    :param languages: set of languages
    :return: list of dicts
    """
    # local param
    expected_number_of_keywords = 30
    # prepare stop words
    stop_words = []
    for language in languages:
        stop_words += get_stopwords_list(language)
    # obtain keywords
    keywords = parse_keywords(stop_words, text)
    # prepare keywords rate
    keywords_count = len(keywords)
    keywords_rate = ((keywords.count(kw) / keywords_count * 100, kw)
                     for kw in set(keywords))
    top_keywords = heapq.nlargest(expected_number_of_keywords, keywords_rate)
    # prepare keywords rate
    keywords_data = [
        {"keyword": elem[1], "rate": elem[0]}
        for elem in top_keywords]
    return keywords_data[:expected_number_of_keywords]


class MiniDashGenerator(object):
    """
    Segment mini-dash generator
    """
    def __init__(self, sdb_data):
        """
        Set up procedure
        """
        self.sdb_data = sdb_data
        self.history_length = 31
        # TODO check for potential errors
        self.max_history_date = datetime.strptime(max(
            [obj.get("history_date") for obj in sdb_data]), "%Y-%m-%d")
        self.date_range = prepare_date_range(
            self.max_history_date, self.history_length - 1)

    def get_video_views_chart_data_section(self):
        """
        Prepare video views chart data section
        :return: list
        """
        video_views_data = [
            [0] * (self.max_history_date
                   - datetime.strptime(obj.get("history_date"), "%Y-%m-%d")
                   ).days + get_diff_history_by_period(
                        obj.get("video_views_history")[:self.history_length],
                        days=2)
            for obj in self.sdb_data
            ]
        response = self.prepare_response_data(
            video_views_data, "video_views_count")
        return response

    def get_views_per_video_chart_data_section(self):
        """
        Prepare views per video chart data section
        :return: list
        """
        views_per_video_data = [
            [0] * (self.max_history_date
                   - datetime.strptime(obj.get("history_date"), "%Y-%m-%d")
                   ).days + get_diff_history_by_period(
                    obj.get("views_per_video_history")[:self.history_length],
                    days=2)
            for obj in self.sdb_data
        ]
        response = self.prepare_response_data(
            views_per_video_data, "views_per_video")
        return response

    def prepare_response_data(self, data, counter_name):
        """
        Prepare response data format
        :param data: list of lists with counter values
        :param counter_name: name of counter field
        :return: list of dicts
        """
        aggregated_data = [
            sum(x) for x in zip_longest(*data, fillvalue=0)]\
            [:self.history_length-1]
        response_data = [
            {"date": date, counter_name: value}
            for date, value in zip_longest(
                self.date_range, aggregated_data, fillvalue=0)]
        # drop zero values from history tail
        response_data.reverse()
        empty_values_indexes = []
        for obj in response_data:
            if not obj.get(counter_name):
                empty_values_indexes.append(response_data.index(obj))
            else:
                break
        response = [
            value for (i, value) in enumerate(response_data)
            if i not in empty_values_indexes]
        response.reverse()
        return response

    def get_keywords_section(self):
        """
        Prepare keywords section
        :return: list of dicts
        """
        descriptions = [
            obj.get("description")
            for obj in self.sdb_data
            if obj.get("description") != "Channel has no description"]
        titles = [obj.get("title")
                  for obj in self.sdb_data if obj.get("title") != "No title"]
        # aggregated value
        text = "{} {}".format(
            " ".join(descriptions), " ".join(titles))[:TEXT_LIMIT_LENGTH]
        languages = {obj.get("language") for obj in self.sdb_data}
        keywords = get_top_keywords_from_text(text, languages)
        return keywords

    @property
    def data(self):
        """
        Prepare serialized data
        :return: dict
        """
        data = {
            "views_chart_data": self.get_video_views_chart_data_section(),
            "views_per_video_chart_data":
                self.get_views_per_video_chart_data_section(),
            "keywords": self.get_keywords_section()
        }
        return data
