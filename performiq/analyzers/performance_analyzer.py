from typing import Dict

from .base_analyzer import BaseAnalyzer
from .base_analyzer import ChannelAnalysis
from .constants import AnalysisResultSection
from performiq.models.constants import AnalysisFields


class PerformanceAnalyzer(BaseAnalyzer):
    """
    Analyzes channels based on ad performance metrics
    Once called, this will attempt to analyze all channels given in iq_channel_results parameter
    """
    RESULT_KEY = AnalysisResultSection.PERFORMANCE_RESULT_KEY
    ANALYSIS_FIELDS = {AnalysisFields.CPV, AnalysisFields.CPM, AnalysisFields.CTR, AnalysisFields.VIDEO_VIEW_RATE,
                       AnalysisFields.ACTIVE_VIEW_VIEWABILITY, AnalysisFields.VIDEO_QUARTILE_100_RATE}

    def __init__(self, params: dict):
        self.params = params
        # If a channel fails in any metric, it fails entirely
        self._failed_channels_count = 0
        self._seen = 0
        # Keep track of counts for each metric being analyzed. Data may not always include an analysis_field,
        # so use actual number of data that has metric to calculate overall performance
        self._total_results = {
            key: dict(failed=0, passed=0) for key in self.ANALYSIS_FIELDS
        }
        # Keep track of sum values to calculate overall averages in self.get_results method
        self._averages = {
            AnalysisFields.CPM: 0,
            AnalysisFields.CPV: 0,
        }

    def analyze(self, channel_analysis: ChannelAnalysis):
        """
        Count of passed and failed items will be tracked for each field in ANALYSIS_FIELDS
        After gathering all results, add performance score using _add_performance_percentage_result method
        """
        # Initialize empty results for all possible performance fields
        curr_result = {
            "passed": True,
            **{
                metric_name: None for metric_name in self.ANALYSIS_FIELDS
            }
        }
        # Ensure that the analysis ran at least for one metric and should contribute to the overall count of items seen
        analyzed = False
        # Compare each metric (e.g. cpm) against thresholds in IQCampaign.params
        for metric_name in self.ANALYSIS_FIELDS:
            threshold = self.params.get(metric_name)
            metric_value = channel_analysis.get(metric_name)
            if not threshold or metric_value is None:
                continue
            self._add_averages(metric_name, metric_value)
            try:
                if self.passes(metric_value, threshold):
                    self._total_results[metric_name]["passed"] += 1
                else:
                    self._total_results[metric_name]["failed"] += 1
                    channel_analysis.clean = False
                    curr_result["passed"] = False
                # Store the actual metric value
                curr_result[metric_name] = metric_value
            except TypeError:
                continue
            else:
                analyzed = True
        # Channel should not have passed = True / False if no fields were analyzed
        if analyzed is False:
            curr_result["passed"] = None
        else:
        # Increment totals to calculate overall score in get_results method
            self._seen += 1
        if curr_result["passed"] is False:
            self._failed_channels_count += 1
        return curr_result

    def get_results(self) -> dict:
        """
        Gather and format results for all channels analyzed in self.analyze method
        :return: dict
            failed: Total channels failed for metric
            passed: Total channels passed for metric
            performance: Percentage passed over all scored
            avg: Average of values for all channels seen

            example_result = {
                "average_cpm": {
                    "failed": 0,
                    "passed": 121,
                    "performance": 100.0,
                    "avg": 3.926
                },
                "video_view_rate": {
                    "failed": 0,
                    "passed": 121,
                    "performance": 100.0
                },
                "ctr": {
                    "failed": 121,
                    "passed": 0,
                    "performance": 0.0
                },
                "active_view_viewability": {
                    "failed": 0,
                    "passed": 0,
                    "performance": null
                },
                "video_quartile_100_rate": {
                    "failed": 0,
                    "passed": 121,
                    "performance": 100.0
                },
                "average_cpv": {
                    "failed": 121,
                    "passed": 0,
                    "performance": 0.0,
                    "avg": 0.0069
                },
                "overall_score": 60.0
            }
        """
        self._add_performance_percentage_results()
        # Check if params were applied for analysis
        params_exist = any(self.params.get(field) is not None for field in self.ANALYSIS_FIELDS)
        overall_score = self.get_score(self._seen - self._failed_channels_count, self._seen) if params_exist else None
        averages = self._calculate_averages()
        self._total_results["overall_score"] = overall_score
        for metric_name, average in averages.items():
            self._total_results[metric_name]["avg"] = average
            self._total_results[metric_name]["avg"] = average
        return self._total_results

    def _add_performance_percentage_results(self) -> None:
        """
        Add percentage results with performance key to results for each metric defined in self.params
        """
        for metric_name, result in self._total_results.items():
            if self.params.get(metric_name):
                passed, failed = result.get("passed", 0), result.get("failed", 0)
                performance = self.get_score(passed, passed + failed)
            else:
                # Threshold value was not saved for current IQCampaign
                performance = None
            self._total_results[metric_name]["performance"] = performance

    def _add_averages(self, metric_name: str, metric_value) -> None:
        """
        Add metric value to total to calculate overall averages of all channels analyzed
        :param metric_name: str
        :param metric_value: str
        :return:
        """
        try:
            self._averages[metric_name] += metric_value
        except KeyError:
            pass

    def _calculate_averages(self) -> Dict[str, float]:
        """
        Calculates averages of metrics using all results
        """
        total = self._seen or 1
        for key, average_sum in self._averages.items():
            self._averages[key] = round(average_sum / total, 4)
        return self._averages

    def passes(self, value, threshold, direction="+"):
        """
        Determines if a channel metric value passes comared to threshold value in self.params
        :param direction: str -> Whether to compare less or greater
        :return:
        """
        if direction == "+":
            passed = value >= threshold
        else:
            passed = value < threshold
        return passed
