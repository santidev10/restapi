from collections import defaultdict

from .base_analyzer import BaseAnalyzer
from .base_analyzer import ChannelAnalysis
from .constants import AnalysisFields
from .constants import AnalysisResultSection
from .constants import IGNORE_CONTENT_CATEGORIES


class ContextualAnalyzer(BaseAnalyzer):
    """
    Analyzer to analyze contextual metadata of channels
    A ContextualAnalyzer expects to be used for many channels and overall results are stored until requested by
        calling the get_results property
    """
    TOP_OCCURRENCES_MAX = 5
    RESULT_KEY = AnalysisResultSection.CONTEXTUAL_RESULT_KEY
    ANALYSIS_FIELDS = {AnalysisFields.CONTENT_CATEGORIES, AnalysisFields.LANGUAGES, AnalysisFields.CONTENT_TYPE,
                       AnalysisFields.CONTENT_QUALITY}

    def __init__(self, params: dict):
        """
        :param params: IQCampaign params value
        """
        # Coerce list params to sets as analyzers check for attributes membership as part of analysis
        self.params = {
            key: set(value) if isinstance(value, list) and value is not None else value
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
        # Total count of all placements seen. Used to calculate percentage occurrences
        self._total_count = 0
        # Total count of placements analyzed with valid params
        self._total_analyzed = 0
        self._analyzers = {
            AnalysisFields.CONTENT_CATEGORIES: self._analyze_content_categories,
            AnalysisFields.CONTENT_TYPE: self._analyze_attribute,
            AnalysisFields.CONTENT_QUALITY: self._analyze_attribute,
            AnalysisFields.LANGUAGES: self._analyze_attribute,
        }
        # Sections that should be used to calculate overall score
        self._analyzed_sections = {}

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
                        0: 39.67,
                        1: 7.44
                    },
                    "content_quality": {
                        None: 52.89,
                        2: 19.83,
                        1: 27.27
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
        # Calculate percentage breakdown for languages, content_type, and content_quality analysis by
        # creating sorted list of how often values occur
        for analysis_type in {AnalysisFields.LANGUAGES, AnalysisFields.CONTENT_TYPE, AnalysisFields.CONTENT_QUALITY}:
            counts_field = analysis_type + "_counts"
            counts = self._total_result_counts[counts_field]
            formatted_key = analysis_type.replace("_counts", "_percents")
            # percents will contain percent occurrence of each analysis_type and targeted boolean
            # targeted describes if the value was targeted in params
            # e.g. [{"en": 75, "targeted": True}, {"ko": 50, "targeted": False}, {"ja": 40, "targeted": False}, ...]
            percents = []
            for key in sorted(counts, key=counts.get, reverse=True):
                percent = self.get_score(counts[key], self._total_count)
                # param key of -1 denotes None values were targeted for content_quality and content_type
                param_key = -1 if key is None else key
                targeted = param_key in self.params.get(analysis_type, {})
                percents.append({key: percent, "targeted": targeted})
            percentage_results[formatted_key] = percents

        percentage_results["content_categories"] = self._get_content_categories_result()
        # overall_score should be calculated only if params were applied for this analyzer
        params_exist = any(
            len(self.params[field]) > 0 for field in self.ANALYSIS_FIELDS
        )
        passed_count = self._total_count - len(self._failed_channels)
        final_result = {
            "overall_score": self.get_score(passed_count, self._total_analyzed) if params_exist else None,
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
        # Track if channel has been analyzed with valid params as not having params should not negatively impact score
        analyzed = False
        contextual_failed = False
        curr_channel_result = {
            "passed": True
        }
        for params_field, analyze_func in self._analyzers.items():
            raw_value = channel_analysis.get(params_field)
            # content_quality and content_type param of -1 indicates we should check for None values
            if params_field in {AnalysisFields.CONTENT_TYPE, AnalysisFields.CONTENT_QUALITY} and raw_value is None:
                raw_value = -1
            # Get key for current params_field to increment in _total_result_counts
            # e.g. count_field = content_categories_counts
            count_field = params_field + "_counts"
            curr_contextual_failed = analyze_func(raw_value, count_field, params_field)
            curr_channel_result[params_field] = raw_value
            if self.params.get(params_field):
                analyzed = True
                if curr_contextual_failed is True:
                    contextual_failed = True

        if contextual_failed is True:
            channel_analysis.clean = False
            curr_channel_result["passed"] = False
            self._failed_channels.add(channel_analysis.channel_id)

        self._total_count += 1
        if analyzed is True:
            self._total_analyzed += 1
        return curr_channel_result

    def _analyze_multi(self, values: list, count_field: str, params_field: str) -> bool:
        """
        Wrapper method to call _analyze_attribute for attributes that contain multiple values
        Same parameters should be passed as _analyze_attribute
        :return: bool
        """
        contextual_failed = False
        for value in values:
            curr_contextual_failed = self._analyze_attribute(value, count_field, params_field)
            if curr_contextual_failed is True:
                contextual_failed = curr_contextual_failed
        return contextual_failed

    def _analyze_attribute(self, value, count_field: str, params_field: str) -> bool:
        """
        Analyze single attribute by comparing to self.params
        :param count_field: str -> Field in self._total_result_counts to increment
        :param params_field: str -> Field in self.params that should be checked
        :param value: Actual value to analyze and compare against self.params[params_field]
        :return: bool
        """
        contextual_failed = True
        # Keep count of attributes
        # e.g. self._total_result_counts[content_quality_counts][0] += 1
        self._total_result_counts[count_field][value] += 1
        # value == -1, then value being analyzed is None and should not fail analysis
        if not self.params.get(params_field) or value == -1:
            return False
        # Check if value of current analysis matches params
        # e.g. val = "Education" not in self.params[AnalysisFields.CONTENT_CATEGORIES]
        if value in self.params[params_field]:
            contextual_failed = False
        return contextual_failed

    def _analyze_content_categories(self, placement_content_categories: list, count_field: str, *_, **__) -> bool:
        """
        Analyze placement content categories against targeted content categories in self.params
        :param placement_content_categories: list of content categories of placement
        :param count_field: Key of self._total_result_counts to increment category occurrence counts
        """
        contextual_failed = True
        if placement_content_categories is None:
            return False
        elif isinstance(placement_content_categories, str):
            placement_content_categories = {placement_content_categories}
        else:
            placement_content_categories = set(placement_content_categories)
        # Increment category occurrences
        for category in placement_content_categories:
            if (category or "").lower() in IGNORE_CONTENT_CATEGORIES:
                continue
            if category in self.params[AnalysisFields.CONTENT_CATEGORIES]:
                # Passes if at least one category matches
                contextual_failed = False
            self._total_result_counts[count_field][category] += 1
        # Increment total counter of matched content categories. Should be incremented only once if any matched
        if contextual_failed is False:
            self._total_result_counts["matched_content_categories"] += 1
        return contextual_failed

    def _get_content_categories_result(self) -> dict:
        """
        Format counts of all content categories analyzed
        """
        # Get top content category occurrences and overall percentage match
        content_categories_counts = self._total_result_counts["content_categories_counts"]
        category_sorted_keys = sorted(
            content_categories_counts, key=content_categories_counts.get, reverse=True)
        category_occurrence = [
            {
                "category": category,
                # Whether or not a category was seen at least once
                "matched": content_categories_counts[category] > 0,
                "targeted": category in self.params[AnalysisFields.CONTENT_CATEGORIES],
                "percent_occurrence": self.get_score(content_categories_counts[category], self._total_count or 1),
            }
            for category in category_sorted_keys
        ]
        result = {
            "category_occurrence": category_occurrence,
            "total_matched_percent": self.get_score(
                self._total_result_counts["matched_content_categories"], self._total_count
            )
        }
        return result
