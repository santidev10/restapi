from collections import defaultdict
from operator import attrgetter
from typing import Dict

from .base_analyzer import BaseAnalyzer
from .base_analyzer import ChannelAnalysis
from .constants import AnalysisFields
from .constants import AnalyzeSection


class ContextualAnalyzer(BaseAnalyzer):
    """
    Analyzer to analyze contextual metadata of channels
    A ContextualAnalyzer expects to be used for many channels and results are stored until requested by accessing
        the results property.
    """
    RESULT_KEY = AnalyzeSection.CONTEXTUAL_RESULT_KEY
    ANALYSIS_FIELDS = {AnalysisFields.CONTENT_CATEGORIES, AnalysisFields.LANGUAGES, AnalysisFields.CONTENT_TYPE,
                       AnalysisFields.CONTENT_QUALITY}

    def __init__(self, params):
        self.params = params
        self._failed_channels = set()
        self._total_result_counts = dict(
            languages_counts=defaultdict(int),
            content_categories_counts=defaultdict(int),
            content_quality_counts=defaultdict(int),
            content_type_counts=defaultdict(int),
            matched_content_categories=0, # Convenience key to keep track how many content categories have matched
        )
        self._seen = 0

    def get_results(self) -> dict:
        """
        Finalize results format
        This method will map result counts into percentages and calculate overall score for all channels processed
            before accessing this property
        :return:
        """
        percentage_results = {}
        passed_count = self._seen - len(self._failed_channels)
        # For each result type (e.g. category, languages) we need to calculate the percentage occurrence of each
        # value in each result type. For languages, we may have analyzed 100 channels with 70 "en" and 30 "fr"
        # languages, so "en" has 70% and fr has 30%
        for result_type, counts in self._total_result_counts.items():
            if "_counts" in result_type:
                formatted_key = result_type.replace("_counts", "_percents")
                for key, value in counts.items():
                    counts[key] = self.get_score(value, self._seen)
                percentage_results[formatted_key] = counts
        percentage_results["content_categories_percents"]["overall_percentage"] = self.get_score(
            self._total_result_counts["matched_content_categories"], self._seen
        )
        final_result = {
            "overall_score": self.get_score(passed_count, self._seen),
            **percentage_results
        }
        return final_result

    def analyze(self, channel_analysis: ChannelAnalysis):
        contextual_failed = False
        curr_channel_result = {
            "passed": True
        }
        for params_field in self.ANALYSIS_FIELDS:
            if not self.params.get(params_field):
                curr_channel_result["passed"] = None
                continue
            value = channel_analysis.get(params_field)
            count_field = params_field + "_counts"
            # Check if value matches params
            contextual_failed = self._analyze(count_field, params_field, value)
            curr_channel_result[params_field] = value
            self._addl_processing(params_field, value)

        if contextual_failed is True:
            channel_analysis.clean = False
            curr_channel_result["passed"] = False
            self._failed_channels.add(channel_analysis.channel_id)
        self._seen += 1
        # Add the contextual analysis result for the current channel being processed
        return curr_channel_result

    def _analyze(self, count_field, params_field, value):
        value = [value] if value is not isinstance(value, list) else value
        contextual_failed = False
        for val in value:
            self._total_result_counts[count_field][val] += 1
            if contextual_failed is False and val not in self.params[params_field]:
                contextual_failed = True
        return contextual_failed

    def _addl_processing(self, params_field, value):
        """
        Separate additional processing logic from __call__ method for organization
        :return:
        """
        # Keep track of matched content categories to calculate overall percentage match of all channels
        # processed
        if params_field == "content_categories" and value in self.params[params_field]:
            self._total_result_counts["matched_content_categories"] += 1
