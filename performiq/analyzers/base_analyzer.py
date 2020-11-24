class BaseAnalyzer:
    def analyze(self, *args, **kwargs):
        raise NotImplementedError

    def get_results(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def get_score(passed, total):
        try:
            score = round(passed / total * 100, 2)
        except ZeroDivisionError:
            score = 0
        return score


class ChannelAnalysis:
    def __init__(self, channel_id, data=None):
        self.channel_id = channel_id
        self.clean = True
        self._data = data
        self._results = {}

    @property
    def results(self):
        return self._results

    @property
    def meta_data(self):
        return self._data

    def add_data(self, data):
        self._data.update(data)

    def get(self, key, default=None):
        value = self._data.get(key, default)
        return value

    def add_result(self, key, result):
        self._results[key] = result
