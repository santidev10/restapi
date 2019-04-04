class DataProviderMixin(object):
    @staticmethod
    def batch(iterable, length):
        """
        Generator that yields equal sized chunks of iterable
        """
        for i in range(0, len(iterable), length):
            yield iterable[i:i + length]
