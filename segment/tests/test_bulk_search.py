from datetime import datetime
from random import randint
from typing import Type
from typing import Union

import pytz

from es_components.constants import SUBSCRIBERS_FIELD
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import VIEWS_FIELD
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.models.channel import ChannelSectionBrandSafety
from es_components.models.video import VideoSectionBrandSafety
from es_components.query_builder import QueryBuilder
from es_components.tests.utils import ESTestCase
from segment.utils.bulk_search import bulk_search
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class BulkSearchTestCase(ExtendedAPITestCase, ESTestCase):

    def test_search_generator_include_cursor_exclusions_option(self):
        """
        Test that the include_cursor_exclusions option adds back results that
        have an invalid value for the cursor field when True and adds them in
        when False. For example, when sorting by stats.views, a True value
        should include results whose values are null, while a False value
        should exclude these results (which was default behavior before)
        """

        def make_populated_channel(channel_id, has_cursor_field=True):
            """
            create a Channel and populate it, ready for upsert
            """
            channel = Channel(channel_id)
            channel.populate_general_data(
                title=f"channel title {channel_id}",
                country_code="US",
                iab_categories="category",
                emails=[f"channel_{channel_id}@mail.com", ],
            )
            subscribers = randint(1, 999999) if has_cursor_field else None
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

        def make_populated_video(video_id, has_cursor_field=True):
            """
            create a Video and populate it, ready for upsert
            """
            video = Video(video_id)
            video.populate_general_data(
                title=f"video title {video_id}",
                youtube_published_at=datetime(2018, 2, 3, 4, 5, 6,
                                              tzinfo=pytz.utc),
            )
            views = randint(1, 999999) if has_cursor_field else None
            video.populate_stats(
                views=views,
                likes=123,
                dislikes=234,
                comments=345,
            )
            video.brand_safety = VideoSectionBrandSafety(
                overall_score=randint(5, 10),
            )
            return video

        def test_model(model_class: Union[Type[Channel], Type[Video]], populate: callable,
                       manager_class: Union[Type[ChannelManager], Type[VideoManager]], sort: list, cursor_field: str):
            """
            Test the bulk_search method with the specified model class
            (currently Channels or Videos)
            """
            instances = []
            for _ in range(instances_count):
                instance_id = next(int_iterator)
                instance = populate(instance_id)
                instances.append(instance)

            instances_without_cursor_field = []
            for _ in range(instances_no_cursor_field_count):
                instance_id = next(int_iterator)
                instance = populate(instance_id, has_cursor_field=False)
                instances_without_cursor_field.append(instance)

            manager = manager_class(
                sections=(Sections.GENERAL_DATA, Sections.STATS,
                          Sections.BRAND_SAFETY)
            )
            all_instances = instances + instances_without_cursor_field
            manager.upsert(all_instances)

            query = QueryBuilder().build().must().range() \
                .field("brand_safety.overall_score").gte(5).get()
            with_exclusions = []
            for batch in bulk_search(model=model_class, query=query, sort=sort, cursor_field=cursor_field, options=None,
                                     batch_size=1000, include_cursor_exclusions=True):
                for item in batch:
                    with_exclusions.append(item)

            without_exclusions = []
            for batch in bulk_search(model=model_class, query=query, sort=sort, cursor_field=cursor_field, options=None,
                                     batch_size=1000, include_cursor_exclusions=False):
                for item in batch:
                    without_exclusions.append(item)

            self.assertEqual(
                len(with_exclusions),
                instances_count + instances_no_cursor_field_count
            )
            self.assertEqual(
                len(without_exclusions),
                instances_no_cursor_field_count
            )

        # main
        instances_count = 5
        instances_no_cursor_field_count = 5
        model_map = {
            Channel: {
                "populate": make_populated_channel,
                "manager": ChannelManager,
                "sort": [{
                    SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}
                }],
                "cursor_field": SUBSCRIBERS_FIELD,
            },
            Video: {
                "populate": make_populated_video,
                "manager": VideoManager,
                "sort": [{VIEWS_FIELD: {"order": SortDirections.DESCENDING}}],
                "cursor_field": VIEWS_FIELD,
            }
        }

        for model in [Channel, Video]:
            test_model(
                model_class=model,
                manager_class=model_map[model]["manager"],
                populate=model_map[model]["populate"],
                sort=model_map[model]["sort"],
                cursor_field=model_map[model]["cursor_field"],
            )
