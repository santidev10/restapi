class Coercers:
    @staticmethod
    def percentage(val):
        return float(val.strip("%"))

    @staticmethod
    def float(val):
        return float(val)

    @staticmethod
    def cost(val):
        return float(val) / 10**6

    @staticmethod
    def integer(val):
        return int(val)

    @staticmethod
    def raw(val):
        return val
