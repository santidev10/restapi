from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaignChannel


class IQChannelResult:
    """ Class to represent the result of analyzing a single Youtube channel placement """
    def __init__(self, iq_channel: IQCampaignChannel):
        self.iq_channel = iq_channel

    def add_result(self, analyzer: str, data: dict):
        """
        Add key with data to results
        :param analyzer: should be one of these values: performance, contextual, suitability
        :param data: should be the return value of PerformanceAnalyzer, ContextualAnalyzer, or SuitabilityAnalyzer
        """
        if analyzer in ANALYZE_SECTIONS:
            self.iq_channel.results[analyzer] = data
        if data["passed"] is False:
            self.iq_channel.clean = False


class BaseAnalyzer:
    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def get_score(passed, total):
        try:
            score = passed // total * 100
        except ZeroDivisionError:
            score = 0
        return score
