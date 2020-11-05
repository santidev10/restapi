from typing import Dict

from .base_analyzer import IQChannelResult
from .base_analyzer import BaseAnalyzer
from .constants import COERCE_FIELD_FUNCS
from .constants import AnalyzeSection
from .utils import Coercers
from performiq.models.constants import CampaignDataFields


class PerformanceAnalyzer(BaseAnalyzer):
    PERFORMANCE_FIELDS = {CampaignDataFields.CPV, CampaignDataFields.CPM, CampaignDataFields.CTR,
                          CampaignDataFields.VIDEO_VIEW_RATE, CampaignDataFields.ACTIVE_VIEW_VIEWABILITY}

    def __init__(self, iq_campaign, iq_results: Dict[str, IQChannelResult]):
        self.iq_campaign = iq_campaign
        self.analyze_params = self.iq_campaign.params
        self.iq_campaign = iq_campaign
        self.iq_results = iq_results
        self._results = None
        # If channel fails in any metric, it fails entirely
        # This will be set by _init_channel_results in the call method as we first need to retrieve API
        # data to set channel ids
        self._failed_channels = set()
        # Keep track of actual channels analyzed as a channel may not have sufficient data to analyze
        self._analyzed_channels_count = 0

    def __call__(self):
        self._results = self._analyze()
        try:
            overall_score = round((self._analyzed_channels_count - len(self._failed_channels))
                                  / self._analyzed_channels_count) * 100
        except ZeroDivisionError:
            overall_score = 0
        averages = self._calculate_averages()
        self._results["overall_score"] = overall_score
        self._results["cpm"]["avg"] = averages["cpm_avg"]
        self._results["cpv"]["avg"] = averages["cpv_avg"]
        return self._results

    def _calculate_averages(self):
        # index [0] is sum, index[1] is count of items
        cpm = [0, 0]
        cpv = [0, 0]
        for r in self.iq_results.values():
            cpm_val = r.iq_channel.meta_data.get(CampaignDataFields.CPM)
            cpv_val = r.iq_channel.meta_data.get(CampaignDataFields.CPV)
            if cpm_val:
                cpm[0] = cpm[0] + Coercers.cost(cpm_val)
                cpm[1] = cpm[1] + 1
            if cpv_val:
                cpv[0] = cpv[0] + Coercers.cost(cpv_val)
                cpv[1] = cpv[1] + 1
        averages = dict(
            cpm_avg=cpm[0] / cpm[1],
            cpv_avg=cpv[0] / cpv[1],
        )
        return averages

    def _analyze(self):
        total_results = {
            key: dict(failed=0, passed=0) for key in self.PERFORMANCE_FIELDS
        }
        for iq_result in self.iq_results.values():
            iq_channel = iq_result.iq_channel
            curr_result = {}
            # Get the Coercer method to map raw values from the api for comparisons
            data = {
                # Default to using the raw value if method not defined for key
                key: COERCE_FIELD_FUNCS.get(key, Coercers.raw)(val) for key, val in iq_channel.meta_data.copy().items()
            }
            analyzed = False
            for metric_name, threshold in self.analyze_params.items():
                metric_value = data.get(metric_name, None)
                try:
                    if self.passes(metric_value, threshold):
                        total_results[metric_name]["passed"] += 1
                    else:
                        total_results[metric_name]["failed"] += 1
                        self._failed_channels.add(iq_channel.channel_id)
                        self.iq_results[iq_channel.channel_id].fail()
                    curr_result[metric_name] = metric_value
                    analyzed = True
                except TypeError:
                    continue
            iq_result.add_result(AnalyzeSection.PERFORMANCE_RESULT_KEY, curr_result)
            if analyzed is True:
                self._analyzed_channels_count += 1
        return dict(total_results)

    def passes(self, value, threshold, direction="+"):
        if direction == "+":
            passed = value > threshold
        else:
            passed = value < threshold
        return passed