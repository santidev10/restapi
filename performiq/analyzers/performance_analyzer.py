from typing import Dict

from .base_analyzer import BaseAnalyzer
from .base_analyzer import ChannelAnalysis
from .constants import AnalyzeSection
from performiq.models.constants import AnalysisFields


class PerformanceAnalyzer(BaseAnalyzer):
    """
    Analyzes channels based on ad performance metrics
    Once called, this will attempt to analyze all channels given in iq_channel_results parameter
    """
    RESULT_KEY = AnalyzeSection.PERFORMANCE_RESULT_KEY
    ANALYSIS_FIELDS = {AnalysisFields.CPV, AnalysisFields.CPM, AnalysisFields.CTR, AnalysisFields.VIDEO_VIEW_RATE,
                       AnalysisFields.ACTIVE_VIEW_VIEWABILITY, AnalysisFields.VIDEO_QUARTILE_100_RATE}

    def __init__(self, params):
        self.params = params
        # This will be set by analyze method
        self._performance_results = {}
        # If channel fails in any metric, it fails entirely
        self._failed_channels_count = 0
        self._seen = 0
        # Keep track of counts for each metric being analyzed. Data may not always include an analysis_field,
        # so use actual number of data that has metric to calculate overall performance
        self._total_results = {
            key: dict(failed=0, passed=0) for key in self.ANALYSIS_FIELDS
        }
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
            "passed": None,
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
            # Add values to calculate overall averages
            self._add_averages(metric_name, metric_value)
            try:
                if self.passes(metric_value, threshold):
                    self._total_results[metric_name]["passed"] += 1
                    curr_result["passed"] = True
                else:
                    channel_analysis.clean = False
                    self._total_results[metric_name]["failed"] += 1
                    curr_result["passed"] = False
                # Store the actual metric value
                curr_result[metric_name] = metric_value
            except TypeError:
                continue
            else:
                analyzed = True
                if curr_result["passed"] is False:
                    self._failed_channels_count += 1
                if analyzed is True:
                    self._seen += 1
        return curr_result

    def get_results(self):
        """
        Get results of performance analysis for all channels in self.iq_channel_results
        :return: dict
        """
        self._add_performance_percentage_results()
        overall_score = self.get_score(self._seen - self._failed_channels_count, self._seen)
        averages = self._calculate_averages()
        self._total_results["overall_score"] = overall_score
        for metric_name, average in averages.items():
            self._total_results[metric_name]["avg"] = average
            self._total_results[metric_name]["avg"] = average
        return self._total_results

    def _add_performance_percentage_results(self):
        """
        Add performance key to results for each metric defined in IQCampaign.params
        """
        for metric_name, result in self._total_results.items():
            if metric_name in self.params:
                passed, failed = result.get("passed", 0), result.get("failed", 0)
                # If no passed and failed, then none were processed
                performance = self.get_score(passed, passed + failed)
            else:
                # Threshold value was not saved for current IQCampaign
                performance = None
            self._total_results[metric_name]["performance"] = performance

    def _add_averages(self, metric_name, metric_value):
        try:
            self._averages[metric_name] += metric_value
        except KeyError:
            pass

    def _calculate_averages(self):
        """
        Calculates averages of metrics using all results
        """
        total = self._seen or 1
        for key, average_sum in self._averages.items():
            self._averages[key] = round(average_sum / total, 4)
        return self._averages

    def passes(self, value, threshold, direction="+"):
        if direction == "+":
            passed = value > threshold
        else:
            passed = value < threshold
        return passed
