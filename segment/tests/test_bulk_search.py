from datetime import datetime
from es_components.constants import Sections, SUBSCRIBERS_FIELD, SortDirections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.models.channel import ChannelSectionBrandSafety
from es_components.query_builder import QueryBuilder
from es_components.tests.utils import ESTestCase
from random import randint
from segment.models.persistent.constants import CHANNEL_SOURCE_FIELDS
from segment.utils.bulk_search import bulk_search
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase
import pytz


class BulkSearchTestCase(ExtendedAPITestCase, ESTestCase):

    def test_search_generator_include_cursor_exclusions_option(self):
        """
        Test that the include_cursor_exclusions option adds back results that
        have an invalid value for the cursor field when True and adds them in
        when False. For example, when sorting by stats.views, a True value
        should include results whose values are null, while a False value
        should exclude these results (which was default behavior before)
        """
        channels_count = 5
        channels_no_subs_count = 5

        def make_populated_channel(channel_id, has_subscribers=True):
            channel = Channel(channel_id)
            channel.populate_general_data(
                title=f"channel title {channel_id}",
                country="country",
                iab_categories="category",
                emails=[f"channel_{channel_id}@mail.com", ],
            )
            subscribers = randint(1, 999999) if has_subscribers else None
            channel.populate_stats(
                subscribers=subscribers,
                last_30day_subscribers=12,
                last_30day_views=321,
                last_7day_views=101,
                last_day_views=20,
                views=randint(2001, 99999),
                views_per_video=123.4,
                sentiment=0.23,
                total_videos_count=10,
                engage_rate=0.34,
                last_video_published_at=datetime(2018, 2, 3, 4, 5, 6,
                                                 tzinfo=pytz.utc),
            )
            channel.brand_safety = ChannelSectionBrandSafety(
                overall_score=randint(5, 10)
            )
            return channel

        channels = []
        for i in range(channels_count):
            channel_id = next(int_iterator)
            channel = make_populated_channel(channel_id)
            channels.append(channel)

        channels_without_subscribers = []
        for i in range(channels_no_subs_count):
            channel_id = next(int_iterator)
            channel = make_populated_channel(channel_id, has_subscribers=False)
            channels_without_subscribers.append(channel)

        manager = ChannelManager(
            sections=(Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)
        )
        all_channels = channels + channels_without_subscribers
        manager.upsert(all_channels)

        query = QueryBuilder().build().must().range().field("stats.views") \
            .gte(50).get()
        with_exclusions = []
        for batch in bulk_search(
            model=Channel,
            query=query,
            sort=[{SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}],
            cursor_field=SUBSCRIBERS_FIELD,
            options=None,
            batch_size=1000,
            source=CHANNEL_SOURCE_FIELDS,
            include_cursor_exclusions=True
        ):
            for item in batch:
                with_exclusions.append(item)

        without_exclusions = []
        for batch in bulk_search(
                model=Channel,
                query=query,
                sort=[{SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}],
                cursor_field=SUBSCRIBERS_FIELD,
                options=None,
                batch_size=1000,
                source=CHANNEL_SOURCE_FIELDS,
                include_cursor_exclusions=False
        ):
            for item in batch:
                without_exclusions.append(item)

        self.assertEqual(len(with_exclusions), channels_count + channels_no_subs_count)
        self.assertEqual(len(without_exclusions), channels_no_subs_count)
