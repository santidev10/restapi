from .base_analyzer import BaseAnalyzer
from .base_analyzer import ChannelAnalysis
from .constants import AnalysisResultSection


class SuitabilityAnalyzer(BaseAnalyzer):
    RESULT_KEY = AnalysisResultSection.SUITABILITY_RESULT_KEY

    def __init__(self, params):
        """
        :param params: IQCampaign params value
        """
        # Coerce list params to sets as analyzers check for attributes membership as part of analysis
        self.params = {
            key: set(value) if isinstance(value, list) and value is not None else value
            for key, value in params.items()
        }
        self._failed_channels = set()
        self._result_counts = dict(
            passed=0,
            failed=0
        )
        self._analyzers = (
            self._analyze_score, self._analyze_categories
        )

    def get_results(self) -> dict:
        """
        Gather and format results for all channels analyzed in self.analyze method
        :return: dict
            passed: total channels that passed analysis
            failed: total channels that failed analysis
            overall_score: Percentage of passed channels to total scored
            example_result: {
                "passed": 121,
                "failed": 0,
                "overall_score": 100.0
            }
        """
        total_count = self._result_counts["passed"] + self._result_counts["failed"]
        self._result_counts["overall_score"] = self.get_score(self._result_counts["passed"], total_count)
        return self._result_counts

    def analyze(self, channel_analysis: ChannelAnalysis):
        curr_channel_result = {
            "passed": True
        }
        analyzed = False
        suitable = True
        # Each analyzer will mutate curr_channel_result by adding metadata of the channel_analysis
        for analyzer in self._analyzers:
            curr_analyzed, curr_suitable = analyzer(channel_analysis, curr_channel_result)
            # Set analyzed to True if any analyzer returns True. Should not change analyzed from True to False
            analyzed = True if curr_analyzed is True else analyzed
            # Set suitable to False if any analyzer returns False. Should not change suitable from False to True
            suitable = False if curr_suitable is False else suitable
        if analyzed is True:
            if suitable is True:
                self._result_counts["passed"] += 1
            else:
                channel_analysis.clean = False
                curr_channel_result["passed"] = False
                self._result_counts["failed"] += 1
                self._failed_channels.add(channel_analysis.channel_id)
        else:
            curr_channel_result.update({
                "overall_score": None,
                "excluded_categories": [],
                "passed": None,
            })
        return curr_channel_result

    def _analyze_score(self, channel_analysis: ChannelAnalysis, channel_result: dict):
        analyzed = False
        suitable = True
        try:
            channel_brand_safety_score = channel_analysis.get("overall_score")
            if channel_brand_safety_score >= self.params["score_threshold"]:
                suitable = True
            else:
                suitable = False
            channel_result["overall_score"] = channel_brand_safety_score
            analyzed = True
        except TypeError:
            pass
        return analyzed, suitable

    def _analyze_categories(self, channel_analysis: ChannelAnalysis, channel_result: dict):
        """
        Analyze content categories by checking if categories match any excluded content categories params
        """
        analyzed = False
        suitable = True
        try:
            matched_excluded_categories = self.params["exclude_content_categories"]\
                .intersection(set(channel_analysis.get("content_categories")))
            if matched_excluded_categories:
                suitable = False
            channel_result["exclude_content_categories"] = list(matched_excluded_categories)
            analyzed = True
        except TypeError:
            pass
        return analyzed, suitable

