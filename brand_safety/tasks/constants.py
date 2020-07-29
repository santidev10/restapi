from datetime import timedelta

CHANNEL_FIELDS = ("main.id", "general_data.title", "general_data.description", "general_data.video_tags",
                  "brand_safety.updated_at")
VIDEO_FIELDS = ("main.id", "general_data.title", "general_data.description", "general_data.tags",
                "general_data.lang_code", "channel.id", "channel.title", "captions", "custom_captions")


class BaseScheduler:
    NAME = None
    TASK_EXPIRATION = dict(hours=2)
    TASK_BATCH_SIZE = 100
    MAX_QUEUE_SIZE = 50

    @classmethod
    def get_expiration(cls):
        expiration = timedelta(**cls.TASK_EXPIRATION).total_seconds()
        return expiration

    @classmethod
    def get_items_limit(cls, curr_queue_size):
        limit = (cls.MAX_QUEUE_SIZE - curr_queue_size) * cls.TASK_BATCH_SIZE
        if limit < 0:
            limit = 0
        return limit


class Schedulers:
    class ChannelDiscovery(BaseScheduler):
        NAME = "brand_safety_channel_discovery"

    class ChannelOutdated(BaseScheduler):
        NAME = "brand_safety_channel_outdated"
        UPDATE_TIME_THRESHOLD = "now-7d/d"

    class VideoDiscovery(BaseScheduler):
        TASK_BATCH_SIZE = 5000
        NAME = "brand_safety_video_discovery"
