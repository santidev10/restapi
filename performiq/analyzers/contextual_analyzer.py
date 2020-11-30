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
    TOP_OCCURRENCES_MAX = 5
    RESULT_KEY = AnalyzeSection.CONTEXTUAL_RESULT_KEY
    ANALYSIS_FIELDS = {AnalysisFields.CONTENT_CATEGORIES, AnalysisFields.LANGUAGES, AnalysisFields.CONTENT_TYPE,
                       AnalysisFields.CONTENT_QUALITY}

    def __init__(self, params: dict):
        # Coerce list params to sets as ContextualAnalyzer checks for attributes membership as part of analysis
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
            # Keep track of how channels have content categories that matched. Channels generally have
            # multiple categories and this should only be incremented when at least one category matches
            matched_content_categories=0,
        )
        self._seen = 0

    def get_results(self) -> dict:
        """
        Gather and format results for all channels analyzed in self.analyze method
        Calculates overall performance for all channel analyses processed by analyze method
        :return: dict
            overall_score: Overall score for all channels seen in self.analyze method. Simple percentage of passed / totla
            content_type: Percentage breakdown of content_type values e.g.content_quality["2"] = Percentage of channels
                that have content_type value of "2"
            content_quality: Percentage breakdown of content_quality values. Calculated similarly to content_type
            content_categories: Top occurrences of content categories and overall matched percentage
                example_result = {
                    "overall_score": 95.87,
                    "content_type": {
                        None: 52.89,
                        "0": 39.67,
                        "1": 7.44
                    },
                    "content_quality": {
                        None: 52.89,
                        "2": 19.83,
                        "1": 27.27
                    },
                    "content_categories": {
                        "top_occurrence": [
                            "Action Video Games",
                            "Arts & Crafts",
                            "Children's Music",
                            "Volleyball",
                            "Softball"
                        ],
                        "matched": 0.83
                    }
                }
        """
        percentage_results = {}
        passed_count = self._seen - len(self._failed_channels)
        # Calculate percentage breakdown for languages, content_type, and content_quality analysis by
        # creating sorted list of how often values occur
        for analysis_type in {AnalysisFields.LANGUAGES, AnalysisFields.CONTENT_TYPE, AnalysisFields.CONTENT_QUALITY}:
            counts_field = analysis_type + "_counts"
            counts = self._total_result_counts[counts_field]
            formatted_key = analysis_type.replace("_counts", "_percents")
            # percents will contain percent occurrence of each analysis_type and targeted boolean
            # targeted describes if the value was targeted in params
            # e.g. [{"en": 75}, {"ko": 50}, {"ja": 40}, ...]
            percents = []
            for key in sorted(counts, key=counts.get, reverse=True):
                percent = self.get_score(counts[key], self._seen)
                targeted = str(key) in self._params.get(analysis_type, {})
                percents.append({key: percent, "targeted": targeted})
            percentage_results[formatted_key] = percents

        # Get top content category occurrences and overall percentage match
        content_categories_counts = self._total_result_counts["content_categories_counts"]
        top_category_occurrence = sorted(
            content_categories_counts, key=content_categories_counts.get, reverse=True)[:self.TOP_OCCURRENCES_MAX]
        percentage_results["content_categories"] = {
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

    def analyze(self, channel_analysis: ChannelAnalysis) -> dict:
        """
        Analyzes a single Channel for attributes defined in self.ANALYSIS_FIELDS
        If any attribute of Channel does not match, entire contextual analysis is considered failed

        :return: dict -> Results of contextual analysis for channel_analysis
            passed: Whether or not analysis passed
            content_quality: content_quality value of Channel
            languages: languages value of Channel
            content_type: content_type value of Channel
            content_categories: content_categories values of Channel

            example_result = {
                "passed": false,
                "content_quality": 1,
                "languages": "es",
                "content_categories": [
                    "Kids Content",
                    "Pop Culture"
                ],
                "content_type": 0
            }
        """
        contextual_failed = False
        curr_channel_result = {
            "passed": True
        }
        for params_field in self.ANALYSIS_FIELDS:
            raw_value = channel_analysis.get(params_field)
            # Check AttrList from elasticsearch_dsl AttrList type
            mapped_value = [raw_value] if type(raw_value) not in {list, AttrList} else raw_value
            mapped_value = [str(val) if val is not None else val for val in mapped_value]

            count_field = params_field + "_counts"
            # Check if value matches params
            curr_contextual_failed = self._analyze(count_field, params_field, mapped_value)
            if curr_contextual_failed is True:
                contextual_failed = True
            curr_channel_result[params_field] = mapped_value

        if contextual_failed is True:
            channel_analysis.clean = False
            curr_channel_result["passed"] = False
            self._failed_channels.add(channel_analysis.channel_id)
        self._seen += 1
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
        contextual_failed = False
        content_category_matched = False
        for val in value:
            self._total_result_counts[count_field][val] += 1
            # Unable to analyze if param not defined, however we still want to count the occurrences of a value
            if not self._params.get(params_field):
                continue

            if contextual_failed is False and val not in self._params[params_field]:
                contextual_failed = True

            # Channel has at least one content category that matched
            if params_field == AnalysisFields.CONTENT_CATEGORIES and val in self._params[params_field]:
                content_category_matched = True

        if content_category_matched is True:
            self._total_result_counts["matched_content_categories"] += 1
        return contextual_failed
