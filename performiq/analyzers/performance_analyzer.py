from typing import Dict

from .base_analyzer import IQChannelResult
from .base_analyzer import BaseAnalyzer
from .constants import AnalyzeSection
from performiq.models import IQCampaign
from performiq.models.constants import CampaignDataFields


class PerformanceAnalyzer(BaseAnalyzer):
    """
    Analyzes channels based on ad performance metrics
    Once called, this will attempt to analyze all channels given in iq_channel_results parameter
    """
    PERFORMANCE_FIELDS = {CampaignDataFields.CPV, CampaignDataFields.CPM, CampaignDataFields.CTR,
                          CampaignDataFields.VIDEO_VIEW_RATE, CampaignDataFields.ACTIVE_VIEW_VIEWABILITY}

    def __init__(self, iq_campaign: IQCampaign, iq_channel_results: Dict[str, IQChannelResult]):
        self.iq_campaign = iq_campaign
        self.iq_channel_results = iq_channel_results
        # This will be set by analyze method
        self._performance_results = None
        # If channel fails in any metric, it fails entirely
        self._failed_channels_count = 0

    def get_results(self):
        """
        Get results of performance analysis for all channels in self.iq_channel_results
        :return: dict
        """
        overall_score = self.get_score(len(self.iq_channel_results) - self._failed_channels_count,
                                       len(self.iq_channel_results))
        averages = self._calculate_averages()
        self._performance_results["overall_score"] = overall_score
        self._performance_results["cpm"]["avg"] = averages["cpm_avg"]
        self._performance_results["cpv"]["avg"] = averages["cpv_avg"]
        return self._performance_results

    def _add_performance_percentage_result(self, results: dict):
        """
        Add performance key to results for each metric defined in IQCampaign.params
        :param results: dict -> Results gathered in _analyze method
        """
        for metric_name, result in results.items():
            if metric_name in self.iq_campaign.params:
                passed, failed = result.get("passed", 0), result.get("failed", 0)
                # If no passed and failed, then none were processed
                performance = self.get_score(passed, passed + failed)
            else:
                # Threshold value was not saved for current IQCampaign
                performance = None
            results[metric_name]["performance"] = performance

    def _calculate_averages(self):
        """
        Calculates averages of metrics using all results
        """
        # Lists to keep track of metrics. index [0] is sum, index[1] is count of items
        cpm = [0, 0]
        cpv = [0, 0]
        for r in self.iq_channel_results.values():
            cpm_val = r.iq_channel.meta_data.get(CampaignDataFields.CPM, 0)
            cpv_val = r.iq_channel.meta_data.get(CampaignDataFields.CPV, 0)
            if cpm_val:
                cpm[0] = cpm[0] + cpm_val
                cpm[1] = cpm[1] + 1
            if cpv_val:
                cpv[0] = cpv[0] + cpv_val
                cpv[1] = cpv[1] + 1
        averages = dict(
            cpm_avg=cpm[0] / cpm[1] or 1,
            cpv_avg=cpv[0] / cpv[1] or 1,
        )
        return averages

    def analyze(self):
        """
        Analyzes for performance using thresholds defined in IQCampaign.params for each channel 
            in self.iq_channel_results
        Count of passed and failed items will be tracked for each field in PERFORMANCE_FIELDS
        After gathering all results, add performance score using _add_performance_percentage_result method
        """
        # Keep track of counts for each metric being analyzed
        total_results = {
            key: dict(failed=0, passed=0) for key in self.PERFORMANCE_FIELDS
        }
        for iq_result in self.iq_channel_results.values():
            iq_channel = iq_result.iq_channel
            # Initialize empty results for all possible performance fields
            curr_result = {
                "passed": None,
                **{
                    metric_name: None for metric_name in self.PERFORMANCE_FIELDS
                }
            }
            data = iq_channel.meta_data.copy()
            for metric_name in self.PERFORMANCE_FIELDS:
                threshold = self.iq_campaign.params.get(metric_name)
                if not threshold:
                    continue
                metric_value = data.get(metric_name, None)
                try:
                    if self.passes(metric_value, threshold):
                        total_results[metric_name]["passed"] += 1
                        curr_result["passed"] = True
                    else:
                        total_results[metric_name]["failed"] += 1
                        curr_result["passed"] = False
                    # Store the actual metric value
                    curr_result[metric_name] = metric_value
                except TypeError:
                    continue
            if not curr_result["passed"]:
                self._failed_channels_count += 1
            # Add the performance analysis result for the current channel being processed
            iq_result.add_result(AnalyzeSection.PERFORMANCE_RESULT_KEY, curr_result)
        self._add_performance_percentage_result(total_results)
        self._performance_results = dict(total_results)

    def passes(self, value, threshold, direction="+"):
        if direction == "+":
            passed = value > threshold
        else:
            passed = value < threshold
        return passed
