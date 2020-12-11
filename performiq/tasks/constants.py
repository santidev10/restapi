from performiq.analyzers.constants import DataSourceType


class BaseScheduler:
    NAME = None
    TASK_EXPIRATION = dict(hours=1)
    MAX_QUEUE_SIZE = 10

    @classmethod
    def get_items_limit(cls, curr_queue_size):
        limit = (cls.MAX_QUEUE_SIZE - curr_queue_size)
        if limit < 0:
            limit = 0
        return limit


class Schedulers:
    class GoogleAdsUpdateScheduler(BaseScheduler):
        NAME = "performiq_google_ads_update_scheduler"


EXPORT_TYPES = {
    DataSourceType.GOOGLE_ADS.value: "Google Ads",
    DataSourceType.DV360.value: "DV360",
    DataSourceType.CSV.value: "CSV",
}
