from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaignChannel


class IQChannelResult:
    def __init__(self, iq_channel: IQCampaignChannel):
        self.iq_channel = iq_channel
        self.results = {}

    def add_result(self, results_key, data):
        self.results[results_key] = data

    def fail(self, section=None):
        self.iq_channel.clean = False
        if section and section in ANALYZE_SECTIONS:
            self.iq_channel.results[section]["pass"] = False


class BaseAnalyzer:

    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def get_overall_score(passed, total):
        try:
            overall_score = passed // total * 100
        except ZeroDivisionError:
            overall_score = 0
        return overall_score
