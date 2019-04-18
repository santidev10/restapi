import re
from collections import Counter
from collections import namedtuple

import langid

KeywordHit = namedtuple("KeywordHit", "name location")


class Audit(object):
    """
    Class for various Audit types to compose common methods with
    """
    def audit(self, text, location, keyword_processor):
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
    def get_dislike_ratio(likes, dislikes):
        """
        Calculate Youtube dislike to like ratio
        :return:
        """
        try:
            ratio = dislikes / (likes + dislikes)
        except ZeroDivisionError:
            ratio = 0
        return ratio

    @staticmethod
    def get_language(text):
        """
        Analyzes metadata for language using langid module
        :param text: text to analyze
        :return: Language code
        """
        language = langid.classify(text)[0].lower()
        return language

    def audit_emoji(self, text, regexp):
        has_emoji = bool(re.search(regexp, text))
        return has_emoji

    @staticmethod
    def get_keyword_count(items):
        counted = Counter(items)
        return ", ".join(["{}: {}".format(key, value) for key, value in counted.items()])
