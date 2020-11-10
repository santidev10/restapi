from datetime import timedelta
import os

CHANNEL_FIELDS = ("main.id", "general_data.title", "general_data.description", "general_data.video_tags",
                  "brand_safety.updated_at")
VIDEO_FIELDS = ("main.id", "general_data.title", "general_data.description", "general_data.tags",
                "general_data.lang_code", "channel.id", "channel.title", "captions", "custom_captions")


class BaseScheduler:
    NAME = None
    TASK_EXPIRATION = dict(hours=2)
    TASK_BATCH_SIZE = 2
    MAX_QUEUE_SIZE = 15
    # Minimum percentage before task queue should be refilled
    TASK_REQUEUE_THRESHOLD = .20

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

    @classmethod
    def get_minimum_threshold(cls):
        minimum = round(cls.MAX_QUEUE_SIZE * cls.TASK_REQUEUE_THRESHOLD)
        return minimum


class Schedulers:
    class ChannelDiscovery(BaseScheduler):
        NAME = "brand_safety_channel_discovery"
        MAX_QUEUE_SIZE = int(os.getenv("BRAND_SAFETY_CHANNEL_PRIORITY_QUEUE_SIZE", BaseScheduler.MAX_QUEUE_SIZE))
        TASK_BATCH_SIZE = int(os.getenv("BRAND_SAFETY_CHANNEL_PRIORITY_TASK_BATCH_SIZE", BaseScheduler.TASK_BATCH_SIZE))

    class ChannelOutdated(BaseScheduler):
        NAME = "brand_safety_channel_outdated"
        UPDATE_TIME_THRESHOLD = "now-7d/d"
        MAX_QUEUE_SIZE = int(os.getenv("BRAND_SAFETY_CHANNEL_LIGHT_QUEUE_SIZE", BaseScheduler.MAX_QUEUE_SIZE))
        TASK_BATCH_SIZE = int(os.getenv("BRAND_SAFETY_CHANNEL_LIGHT_TASK_BATCH_SIZE", BaseScheduler.TASK_BATCH_SIZE))

    class VideoDiscovery(BaseScheduler):
        NAME = "brand_safety_video_discovery"
        TASK_BATCH_SIZE = int(os.getenv("BRAND_SAFETY_VIDEO_PRIORITY_BATCH_SIZE", 2000))
        MAX_QUEUE_SIZE = int(os.getenv("BRAND_SAFETY_VIDEO_PRIORITY_QUEUE_SIZE", BaseScheduler.MAX_QUEUE_SIZE))
