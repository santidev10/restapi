import re
from collections import Counter

import langid
from emoji import UNICODE_EMOJI

from brand_safety import constants


class Audit(object):
    """
    Base class for various Audit types to inherit shared methods and attributes from
    """
    source = None
    metadata = None
    results = None

    def __init__(self):
        raise NotImplemented

    def execute(self):
        """
        Executes the required audit function defined by the original source data type
        :return:
        """
        audit_sources = {
            constants.SDB: self.run_standard_audit,
            constants.YOUTUBE: self.run_custom_audit,
        }
        try:
            audit_executor = audit_sources[self.source]
            audit_executor()
        except KeyError:
            raise ValueError("Unsupported data {} type.".format(self.source))

    def audit(self, regexp):
        """
        Finds all matches of regexp in audit metadata
        :param regexp: Compiled regular expression to match
        :return:
        """
        metadata = self.metadata
        text = ""
        text += metadata.get("title", "")
        text += metadata.get("description", "")
        text += metadata.get("channel_title", "")
        text += metadata.get("transcript", "")

        if metadata.get("tags"):
            text += " ".join(metadata["tags"])
        hits = re.findall(regexp, text)
        return hits

    def set_keyword_terms(self, keywords, attribute):
        setattr(self, attribute, keywords)

    def get_metadata(self, data, source):
        if source == constants.YOUTUBE:
            metadata = self.get_youtube_metadata(data)
        elif source == constants.SDB:
            metadata = self.get_sdb_metadata(data)
        else:
            raise ValueError("Source type {} unsupported.".format(source))
        return metadata

    def run_standard_audit(self):
        raise NotImplemented

    def run_custom_audit(self):
        raise NotImplemented

    def get_youtube_metadata(self, data):
        raise NotImplemented

    def get_sdb_metadata(self, data):
        raise NotImplemented

    @staticmethod
    def get_language(data):
        """
        Analyzes metadata for language using langid module
        :param data: Youtube data
        :return: Language code
        """
        language = data["snippet"].get("defaultLanguage", None)
        if language is None:
            text = data["snippet"].get("title", "") + data["snippet"].get("description", "")
            language = langid.classify(text)[0].lower()
        return language

    @staticmethod
    def compile_emoji_regexp(unicode):
        regexp = re.compile(
            "({})".format("|".join([r"{}".format(re.escape(word)) for word in unicode]))
        )
        return regexp

    @staticmethod
    def detect_emoji(text):
        has_emoji = text in UNICODE_EMOJI
        return has_emoji

    @staticmethod
    def get_keyword_count(items):
        counted = Counter(items)
        return ", ".join(["{}: {}".format(key, value) for key, value in counted.items()])
