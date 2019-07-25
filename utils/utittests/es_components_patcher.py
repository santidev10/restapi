class ESResultPatcher:
    @property
    def hits(self):
        return []

    def to_dict(self, *args, **kwargs):
        return {}


class SearchDSLPatcher:
    def execute(self):
        return ESResultPatcher()

    def count(self):
        return 0

    def source(self, *args, **kwargs):
        return SearchDSLPatcher()
