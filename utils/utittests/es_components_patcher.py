class ESResultPatcher:
    @property
    def hits(self):
        return []

    def to_dict(self):
        return {}


class SearchDSLPatcher:
    def execute(self):
        return ESResultPatcher()

    def count(self):
        return 0
