import re
from collections import Counter

import langid

import audit_tool.audit_constants as constants


class Audit(object):
    """
    Base class for various Audit types to inherit shared methods and attributes from
    """
    source = None
    metadata = None
    results = None

    emoji_regexp = re.compile(
        u"["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "]", flags=re.UNICODE
    )

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
        Finds all matches of regexp in Youtube data object
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

    def get_export_row(self, audit_type=constants.BRAND_SAFETY):
        """
        Formats exportable csv row using object metadata
            Removes unused metadata before export
        :param audit_type:
        :return:
        """
        row = dict(**self.metadata)
        row.pop("channel_id", None)
        row.pop("video_id", None)
        row.pop("id", None)
        row.pop("tags", None)
        row.pop("transcript", None)
        row = list(row.values())
        audit_hits = self.get_keyword_count(self.results[audit_type])
        row.append(audit_hits)
        return row

    def detect_emoji(self, text):
        has_emoji = bool(re.search(self.emoji_regexp, text))
        return has_emoji

    @staticmethod
    def get_keyword_count(items):
        counted = Counter(items)
        return ", ".join(["{}: {}".format(key, value) for key, value in counted.items()])
