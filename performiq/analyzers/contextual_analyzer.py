from collections import defaultdict

from elasticsearch_dsl.utils import AttrList

from .base_analyzer import BaseAnalyzer
from .base_analyzer import ChannelAnalysis
from .constants import AnalysisFields
from .constants import AnalyzeSection


class ContextualAnalyzer(BaseAnalyzer):
    """
    Analyzer to analyze contextual metadata of channels
    A ContextualAnalyzer expects to be used for many channels and overall results are stored until requested by
        calling the get_results property
    """
    RESULT_KEY = AnalyzeSection.CONTEXTUAL_RESULT_KEY
    ANALYSIS_FIELDS = {AnalysisFields.CONTENT_CATEGORIES, AnalysisFields.LANGUAGES, AnalysisFields.CONTENT_TYPE,
                       AnalysisFields.CONTENT_QUALITY}

    def __init__(self, params: dict):
        # Coerce list params to sets as ContextualAnalyzer checks for membership as part of analysis
        self._params = {
            key: set(value) if isinstance(value, list) else value
            for key, value in params.items()
        }
        self._failed_channels = set()
        self._total_result_counts = dict(
            languages_counts=defaultdict(int),
            content_categories_counts=defaultdict(int),
            content_quality_counts=defaultdict(int),
            content_type_counts=defaultdict(int),
            # Keep track of how channels have content categories that matched
            matched_content_categories=0,
        )
        self._seen = 0

    def get_results(self) -> dict:
        """
        Finalize results by calculating overall performance for all analyses processed by analyze method
        :return:
        """
        percentage_results = {}
        passed_count = self._seen - len(self._failed_channels)
        # Calculate percentage breakdown for content_type and content_quality analysis
        for analysis_type in {AnalysisFields.CONTENT_TYPE, AnalysisFields.CONTENT_QUALITY}:
            counts_field = analysis_type + "_counts"
            counts = self._total_result_counts[counts_field]
            formatted_key = analysis_type.replace("_counts", "_percents")
            for key, count in counts.items():
                counts[key] = self.get_score(count, self._seen)
            percentage_results[formatted_key] = counts

        content_categories_counts = self._total_result_counts["content_categories_counts"]
        top_category_occurrence = sorted(
            content_categories_counts, key=content_categories_counts.get)[:5]
        percentage_results["content_categories_percents"] ={
            "top_occurrence": top_category_occurrence,
            "matched": self.get_score(
                self._total_result_counts["matched_content_categories"], self._seen
            )
        }
        final_result = {
            "overall_score": self.get_score(passed_count, self._seen),
            **percentage_results
        }
        return final_result

    def analyze(self, channel_analysis: ChannelAnalysis):
        contextual_failed = False
        curr_channel_result = {
            "passed": None
        }
        for params_field in self.ANALYSIS_FIELDS:
            if not self._params.get(params_field):
                curr_channel_result["passed"] = None
                continue
            value = channel_analysis.get(params_field)
            count_field = params_field + "_counts"
            # Check if value matches params
            contextual_failed = self._analyze(count_field, params_field, value)
            curr_channel_result[params_field] = value

        if contextual_failed is True:
            channel_analysis.clean = False
            curr_channel_result["passed"] = False
            self._failed_channels.add(channel_analysis.channel_id)
        self._seen += 1
        # Add the contextual analysis result for the current channel being processed
        return curr_channel_result

    def _analyze(self, count_field: str, params_field: str, value) -> bool:
        """
        Analyze single metric
        Set single values as one element lists as values to analyze may have multiple values
            e.g. A channel may have multiple content categories and only one language
        :param count_field: str -> Field in self._total_result_counts to increment
        :param params_field: str -> Field in self._params that should be checked
        :param value: Actual value to analyze and compare against self._params[params_field]
        :return: bool
        """
        # Check AttrList from elasticsearch_dsl AttrList type
        to_list = type(value) not in {list, AttrList}
        value = [value] if to_list else value
        contextual_failed = False
        content_category_matched = False
        for val in value:
            if contextual_failed is False and val not in self._params[params_field]:
                contextual_failed = True
            self._total_result_counts[count_field][val] += 1

            # Channel has at least one content category that matched
            if params_field == AnalysisFields.CONTENT_CATEGORIES and val in self._params[params_field]:
                content_category_matched = True

        if content_category_matched is True:
            self._total_result_counts["matched_content_categories"] += 1
        return contextual_failed
