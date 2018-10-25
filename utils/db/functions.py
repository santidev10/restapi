from django.db.models.functions.datetime import TruncBase


class TruncQuarter(TruncBase):
    kind = 'quarter'


class TruncWeek(TruncBase):
    kind = 'week'
