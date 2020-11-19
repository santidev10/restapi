from operator import attrgetter
from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaignChannel


class BaseAnalyzer:
    def analyze(self, *args, **kwargs):
        raise NotImplementedError

    def get_results(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def get_score(passed, total):
        try:
            score = round(passed / total * 100, 2)
        except ZeroDivisionError:
            score = 0
        return score


class ChannelAnalysis:
    ES_FIELD_MAPPING = {
        "content_categories": "general_data.iab_categories",
        "languages": "general_data.top_lang_code",
        "content_quality": "task_us_data.content_quality",
        "content_type": "task_us_data.content_type"
    }

    def __init__(self, channel_id, dict_data=None, es_channel=None):
        self.channel_id = channel_id
        self.clean = True
        self._dict_data = dict_data
        self._es_channel = es_channel
        self._results = {}

    @property
    def results(self):
        return self._results

    @property
    def meta_data(self):
        return self._dict_data

    def add_dict_data(self, data):
        self._dict_data.update(data)

    def set_es_channel(self, es_channel):
        self._es_channel = es_channel

    def get(self, key):
        value = None
        try:
            value = self._dict_data[key]
        except KeyError:
            pass
        if value is None:
            es_key = self.ES_FIELD_MAPPING[key]
            try:
                value = attrgetter(es_key)(self._es_channel)
            except AttributeError:
                pass
        return value

    def add_result(self, key, result):
        self._results[key] = result
