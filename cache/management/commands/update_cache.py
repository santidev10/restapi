import logging

from django.core.management.base import BaseCommand

from cache.tasks import cache_channel_aggregations
from cache.tasks import cache_research_channels_defaults
from cache.tasks import cache_research_videos_defaults
from cache.tasks import cache_video_aggregations

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **kwargs):
        cache_channel_aggregations.delay()
        cache_video_aggregations.delay()
        cache_research_channels_defaults.delay()
        cache_research_videos_defaults.delay()
