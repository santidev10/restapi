from django.core.management.base import BaseCommand
import logging

from segment.models.persistent import PersistentSegmentRelatedChannel
from utils.utils import chunks_generator
from utils.youtube_api import YoutubeAPIConnector


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        youtube = YoutubeAPIConnector()

        channel_ids = PersistentSegmentRelatedChannel.objects\
            .filter(details__subscribers__isnull=True)\
            .values_list("related_id", flat=True)\
            .distinct()

        page_size = 50
        total_channels = channel_ids.count()
        processed_channels = 0
        resolved_channels = 0
        for chunk in chunks_generator(channel_ids, page_size):
            subscribers_info = self.get_subscribers(youtube, chunk)
            for channel_id, subscribers in subscribers_info:
                self.update_channel_subscribers(channel_id, subscribers)

            processed_channels += page_size
            resolved_channels += len(subscribers_info)

            logging.info("processed_channels={}/{},  resolved={}"
                         .format(processed_channels, total_channels, resolved_channels))

    @staticmethod
    def get_subscribers(youtube, channel_ids):
        response = youtube.obtain_channels(channels_ids=",".join(channel_ids), part="statistics")
        items = response.get("items", [])
        info = [(item.get("id", None), int(item.get("statistics", {}).get("subscriberCount", 0))) for item in items]
        return info

    @staticmethod
    def update_channel_subscribers(channel_id, subscribers):
        queryset = PersistentSegmentRelatedChannel.objects\
            .filter(details__subscribers__isnull=True, related_id=channel_id)
        for channel in queryset:
            if not channel.details:
                channel.details = dict()
            channel.details["subscribers"] = subscribers
            channel.save()
