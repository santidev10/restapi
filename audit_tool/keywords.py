from collections import OrderedDict
from django.http import QueryDict
import logging
import re

from singledb.connector import SingleDatabaseApiConnector


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Keywords:
    _regexp = None
    _regexp_cleanup = re.compile(r"[\W_]+")

    _keywords = None

    def parse_each(self, texts):
        i = 0
        total = len(texts)
        logger.info("Parsing {} item(s)".format(total))
        for text in texts:
            i += 1
            if not i % 1000:
                logger.info("  {} / {}".format(i, total))
            yield self.parse(text)

    def parse(self, text):
        if self._regexp is None:
            self.compile_regexp()
        text = self._regexp_cleanup.sub(".", text).lower()
        keywords = re.findall(self._regexp, text)
        keywords = tuple([self._regexp_cleanup.sub('', kw) for kw in keywords])
        return keywords

    def compile_regexp(self, keywords=None):
        if keywords is None:
            keywords = self.unique()

        kw_regexps = []
        for kw in keywords:
            regexp = "[\s.]*".join([re.escape(char) for char in kw])
            regexp = r"\b{}\b".format(regexp)
            kw_regexps.append(regexp)

        regexp = "({})".format(
            "|".join(kw_regexps)
        )

        self._regexp = re.compile(regexp)

    def unique(self, keywords=None):
        if keywords is None:
            keywords = self.clean()
        keywords = OrderedDict((k, 1) for k in keywords)
        keywords = tuple(keywords.keys())
        return keywords

    def clean(self):
        assert self._keywords is not None
        re_cleanup = re.compile(r"[\W_]+")
        keywords = tuple([
            re_cleanup.sub("", w).lower() for w in self._keywords
        ])
        return keywords

    def load_from_sdb(self):
        params = QueryDict()
        keywords = SingleDatabaseApiConnector().get_bad_words_list(params)
        keywords = [kw.get("name") for kw in keywords]
        self._keywords = keywords

    def load_from_file(self, filename):
        with open(filename, 'r') as file:
            self._keywords = [l.strip() for l in file.readlines()]
