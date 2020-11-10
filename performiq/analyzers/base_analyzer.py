from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaignChannel


class IQChannelResult:
    """ Class to represent the result of analyzing a single Youtube channel placement """
    def __init__(self, iq_channel: IQCampaignChannel):
        self.iq_channel = iq_channel
        self.results = {}

    def add_result(self, analyzer: str, data: dict):
        """
        Add key with data to results
        :param analyzer: should be one of these values: performance, contextual, suitability
        :param data: should be the return value of PerformanceAnalyzer, ContextualAnalyzer, or SuitabilityAnalyzer
        """
        if analyzer in ANALYZE_SECTIONS:
            self.results[analyzer] = data

    def fail(self, analyzer=None):
        """
        Mark result as failed
        :param analyzer: should be one of these values: performance, contextual, suitability
        :return:
        """
        if analyzer and analyzer in ANALYZE_SECTIONS:
            self.iq_channel.clean = False
            self.iq_channel.results[analyzer]["pass"] = False


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
