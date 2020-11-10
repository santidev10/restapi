from typing import Dict

from .base_analyzer import BaseAnalyzer
from .base_analyzer import IQChannelResult
from .constants import AnalyzeSection
from es_components.models import Channel
from performiq.models import IQCampaign


class SuitabilityAnalyzer(BaseAnalyzer):
    def __init__(self, iq_campaign: IQCampaign, iq_results: Dict[str, IQChannelResult]):
        self.iq_campaign = iq_campaign
        self.iq_results = iq_results
        self.analyze_params = iq_campaign.params
        self._failed_channels = set()
        self._result_counts = dict(
            passed=0,
            failed=0
        )

    @property
    def results(self):
        total_count = self._result_counts["passed"] + self._result_counts["failed"]
        self._result_counts["overall_score"] = self.get_score(self._result_counts["passed"], total_count)
        return self._result_counts

    def __call__(self, channel: Channel):
        suitability_failed = False
        result = {"overall_score": None, "passed": True}
        try:
            if channel.brand_safety.overall_score > self.analyze_params["suitability"]:
                self._result_counts["passed"] += 1
            else:
                result["passed"] = False
                self._result_counts["failed"] += 1
                self._failed_channels.add(channel.main.id)
                self.iq_results[channel.main.id].fail()
                suitability_failed = True
            result["overall_score"] = channel.brand_safety.overall_score
        except TypeError:
            return
        self.iq_results[channel.main.id].add_result(AnalyzeSection.SUITABILITY_RESULT_KEY, result)
        return suitability_failed
