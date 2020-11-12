from collections import defaultdict
from operator import attrgetter
from typing import Dict

from .base_analyzer import BaseAnalyzer
from .base_analyzer import IQChannelResult
from .constants import AnalyzeSection
from es_components.models import Channel


class ContextualAnalyzer(BaseAnalyzer):
    """
    Analyzer to analyze contextual metadata of channels
    A ContextualAnalyzer expects to be used for many channels and results are stored until requested by accessing
        the results property.
    """
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
        self._total_result_counts = dict(
            languages_counts=defaultdict(int),
            content_categories_counts=defaultdict(int),
            content_quality_counts=defaultdict(int),
            content_type_counts=defaultdict(int),
            matched_content_categories=0, # Convenience key to keep track how many content categories have matched
        )
        self._analyzed_channels_count = 0

    def get_results(self) -> dict:
        """
        Finalize results format
        This method will map result counts into percentages and calculate overall score for all channels processed
            before accessing this property
        :return:
        """
        percentage_results = {}
        passed_count = self._analyzed_channels_count - len(self._failed_channels)
        # For each result type (e.g. category, languages) we need to calculate the percentage occurrence of each
        # value in each result type. For languages, we may have analyzed 100 channels with 70 "en" and 30 "fr"
        # languages, so "en" has 70% and fr has 30%
        for result_type, counts in self._total_result_counts.items():
            if "_counts" in result_type:
                formatted_key = result_type.replace("_counts", "_percents")
                for key, value in counts.items():
                    counts[key] = self.get_score(value, self._analyzed_channels_count)
                percentage_results[formatted_key] = counts
        percentage_results["content_categories_percents"]["overall_percentage"] = self.get_score(
            self._total_result_counts["matched_content_categories"], self._analyzed_channels_count
        )
        final_result = {
            "overall_score": self.get_score(passed_count, self._analyzed_channels_count),
            **percentage_results
        }
        return final_result

    def analyze(self, channel: Channel) -> None:
        """
        Analyze a single channel by comparing it's metadata values accessed with ES_FIELD_RESULTS_MAPPING and
            the parameters created with the IQCampaign
        This method will mutate the iq_results dictionary passed in during instantiation with the results for the
            current channel being processed
        :param channel: Channel document being analyzed
        :return: dict
        """
        contextual_failed = False
        analyzed = False
        curr_channel_result = {
            "passed": True
        }
        for es_field, params_field in self.ES_FIELD_RESULTS_MAPPING.items():
            # Ensure that we have at least analyzed one available field on document
            analyzed = True
            count_field = params_field + "_counts"
            # Get value of current field e.g. channel.general_data.primary_category
            attr_value = str(attrgetter(es_field)(channel))
            self._total_result_counts[count_field][attr_value] += 1
            # Check if current value is in campaign params e.g. If channel language is in params["languages"]
            # If any metadata value does not match params, the entire channel is considered failed
            if attr_value not in self.analyze_params[params_field]:
                contextual_failed = True
            curr_channel_result[params_field] = attr_value
            self._addl_processing(params_field, attr_value)

        if contextual_failed is True:
            curr_channel_result["passed"] = False
            self._failed_channels.add(channel.main.id)
        if analyzed is True:
            self._analyzed_channels_count += 1
        self.iq_results[channel.main.id].add_result(AnalyzeSection.CONTEXTUAL_RESULT_KEY, curr_channel_result)

    def _addl_processing(self, params_field, value):
        """
        Separate additional processing logic from __call__ method for organization
        :return:
        """
        # Keep track of matched content categories to calculate overall percentage match of all channels
        # processed
        if params_field == self.ES_FIELD_RESULTS_MAPPING["general_data.primary_category"]\
                and value in self.analyze_params[params_field]:
            self._total_result_counts["matched_content_categories"] += 1
