import logging

from django.core.management.base import BaseCommand

from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.query_builder import QueryBuilder

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        manager = ChannelManager(sections=(Sections.CUSTOM_PROPERTIES,),
                                 upsert_sections=(Sections.CUSTOM_PROPERTIES,))
        q1 = QueryBuilder().build().must().exists().field("analytics").get()
        q2 = QueryBuilder().build().must_not().exists().field("custom_properties.preferred").get()
        query = q1 & q2
        verified_channels = manager.search(query, limit=10000).execute().hits
        logger.info(f"Pulled {len(verified_channels)} Channels from Elastic Search.")

        while len(verified_channels) > 0:
            for channel in verified_channels:
                channel.populate_custom_properties(preferred=True)
            manager.upsert(verified_channels)
            logger.info(f"Upserted {len(verified_channels)} Channels to Elastic Search.")
            verified_channels = manager.search(query, limit=10000).execute().hits
            logger.info(f"Pulled {len(verified_channels)} Channels from Elastic Search.")

        logger.info("Finished Migration task.")
