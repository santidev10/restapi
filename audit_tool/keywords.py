import logging
import re
from collections import OrderedDict

from audit_tool.utils.regex_trie import get_optimized_regex
from brand_safety.models import BadWord

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Keywords:
    _regexp_cleanup = re.compile(r"[\W_]+")
    _regexp = None

    _keywords = None

    def __init__(self, source="db", filename=None):
        assert source in ["db", "file"]

        if source == "file":
            assert filename is not None

        if source == "db":
            self.load_from_db()

        elif source == "file":
            self.load_from_file(filename)

    def parse_all(self, texts):
        result = []
        total = len(texts)
        for i, text in enumerate(texts):
            if i % 1000 == 0:
                logger.info(" %d / %d", i, total)
            result.append(self.parse(text))
        return result

    def parse(self, text):
        if self._regexp is None:
            self.compile_regexp()
        keywords = re.findall(self._regexp, text)
        return keywords

    def compile_regexp(self, keywords=None):
        if keywords is None:
            keywords = self._keywords

        self._regexp = get_optimized_regex(words_list=keywords)

    def unique(self, keywords=None):
        if keywords is None:
            keywords = self.clean()
        keywords = OrderedDict((k, 1) for k in keywords)
        keywords = tuple(keywords.keys())
        return keywords

    def clean(self):
        re_cleanup = re.compile(r"[\W_]+")
        keywords = tuple([
            re_cleanup.sub("", w).lower() for w in self._keywords
        ])
        return keywords

    def load_from_db(self):
        bad_words_names = BadWord.objects.values_list("name", flat=True)
        keywords = list(bad_words_names)
        self._keywords = keywords

    def load_from_file(self, filename):
        with open(filename, "r") as file:
            self._keywords = [l.strip() for l in file.readlines()]

    def find_original(self, tag):
        clean = self.clean()
        return self._keywords[clean.index(tag)]
