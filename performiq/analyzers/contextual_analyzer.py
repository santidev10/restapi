from collections import defaultdict
from operator import attrgetter
from typing import Dict

from .base_analyzer import BaseAnalyzer
from .base_analyzer import IQChannelResult
from .constants import AnalyzeSection
from es_components.models import Channel


class ContextualAnalyzer(BaseAnalyzer):
    # Mapping of fieldname on Elasticsearch document to params value to check values are in parameters
    ES_FIELD_RESULTS_MAPPING = {
        "general_data.primary_category": "content_categories",
        "general_data.top_lang_code": "languages",
        "task_us_data.content_quality": "content_quality",
        "task_us_data.content_type": "content_type",
    }

    def __init__(self, iq_campaign, iq_results: Dict[str, IQChannelResult]):
        self.iq_campaign = iq_campaign
        self.iq_results = iq_results
        self.analyze_params = iq_campaign.params
        self._failed_channels = set()
        self._result_counts = dict(
            languages_counts=defaultdict(int),
            content_categories_counts=defaultdict(int),
            content_quality_counts=defaultdict(int),
            content_type_counts=defaultdict(int),
        )
        self._analyzed_channels_count = 0

    @property
    def results(self):
        passed_count = self._analyzed_channels_count - len(self._failed_channels)
        result = {
            "overall_score": self.get_score(passed_count, self._analyzed_channels_count),
            **self._result_counts
        }
        return result

    def __call__(self, channel: Channel):
        contextual_failed = False
        analyzed = False
        result = {
            "passed": True
        }
        for es_field, params_field in self.ES_FIELD_RESULTS_MAPPING.items():
            analyzed = True
            count_field = params_field + "_counts"
            # Get value of current field e.g. channel.general_data.primary_category
            attr_value = str(attrgetter(es_field)(channel))
            self._result_counts[count_field][attr_value] += 1
            # Check if current value is in campaign params e.g. If channel language is in params["languages"]
            if attr_value not in self.analyze_params[params_field]:
                contextual_failed = True
            result[params_field] = attr_value
        if contextual_failed is True:
            result["passed"] = False
            self.iq_results[channel.main.id].fail()
            self._failed_channels.add(channel.main.id)
        if analyzed is True:
            self._analyzed_channels_count += 1
        self.iq_results[channel.main.id].add_result(AnalyzeSection.CONTEXTUAL_RESULT_KEY, result)
        return contextual_failed
