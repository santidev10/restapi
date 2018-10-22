from django.db.models.functions.datetime import TruncBase


class TruncQuarter(TruncBase):
    kind = 'quarter'
