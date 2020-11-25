from .base_analyzer import BaseAnalyzer
from .base_analyzer import ChannelAnalysis
from .constants import AnalyzeSection


class SuitabilityAnalyzer(BaseAnalyzer):
    RESULT_KEY = AnalyzeSection.SUITABILITY_RESULT_KEY

    def __init__(self, params):
        self.params = params
        self._failed_channels = set()
        self._result_counts = dict(
            passed=0,
            failed=0
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
        curr_channel_result = {"overall_score": channel_analysis.get("overall_score"), "passed": True}
        try:
            if curr_channel_result["overall_score"] >= self.params["score_threshold"]:
                self._result_counts["passed"] += 1
            else:
                channel_analysis.clean = False
                curr_channel_result["passed"] = False
                self._result_counts["failed"] += 1
                self._failed_channels.add(channel_analysis.channel_id)
        except TypeError:
            return
        return curr_channel_result
