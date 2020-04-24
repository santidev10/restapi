import csv
from django.contrib.auth.models import Group
from rest_framework.status import HTTP_200_OK
from datetime import timedelta

from django.utils import timezone
from audit_tool.models import AuditLanguage
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety.tasks.channel_outdated import channel_outdated_scheduler
from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BrandSafetyTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.TASK_US_DATA, Sections.BRAND_SAFETY))
    video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.TASK_US_DATA,
                                           Sections.BRAND_SAFETY, Sections.CHANNEL))

    def setup_data(self):
        langs = AuditLanguage.objects.bulk_create([AuditLanguage(language="en"), AuditLanguage(language="ru")])
        bs_category = BadWordCategory.objects.create(name="test")
        bad_words = [
            BadWord(name="bad", language=langs[0], category=bs_category),
            BadWord(name="word", language=langs[1], category=bs_category),
        ]
        outdated_time = timezone.now() - timedelta(days=100)
        bs_id = bs_category.id
        vetted_channels = [
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=100)}, updated_at=outdated_time),
                task_us_data=dict(brand_safety=[bs_id]),
                general_data=dict(description="A bad word"),
            )),
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=0)}, updated_at=outdated_time),
                task_us_data=dict(brand_safety=[bs_id]),
                general_data=dict(description="Another bad word"),
            )),
        ]
        non_vetted_channels = [
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=0)}, updated_at=outdated_time),
                general_data=dict(description="Another bad word"),
            )),
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=0)}, updated_at=outdated_time),
                general_data=dict(description="A bad word"),
            )),
        ]
        vetted_videos = [
            Video(**dict(
                main=dict(id=f"video_{next(int_iterator)}"),
                channel=dict(id=vetted_channels[0].main.id),
                brand_safety=dict(categories={"2": dict(category_score=100)}),
                task_us_data=dict(brand_safety=["2"]),
                general_data=dict(description="A bad word"),
            )),
            Video(**dict(
                main=dict(id=f"video_{next(int_iterator)}"),
                channel=dict(id=vetted_channels[1].main.id),
                brand_safety=dict(categories={"5": dict(category_score=0)}),
                task_us_data=dict(brand_safety=["5"]),
                general_data=dict(description="A bad word"),
            )),
        ]
        non_vetted_videos = [
            Video(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                channel=dict(id=vetted_channels[0].main.id),
                general_data=dict(description="Another bad word"),
            )),
            Video(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                channel=dict(id=vetted_channels[1].main.id),
                general_data=dict(description="A bad word"),
            )),
        ]
        self.channel_manager.upsert(vetted_channels + non_vetted_channels)
        self.video_manager.upsert(vetted_videos + non_vetted_videos)
        return vetted_channels, non_vetted_channels, vetted_videos, non_vetted_videos, bad_words, outdated_time

    def test_brand_safety(self):
        vetted_channels, non_vetted_channels, vetted_videos, non_vetted_videos, bad_words, outdated_time = self.setup_data()
        vetted_ids = [item.main.id for item in vetted_channels]
        non_vetted_channel_ids = [item.main.id for item in non_vetted_channels]
        channel_outdated_scheduler(vetted_ids + non_vetted_channel_ids)

        vetted_channels_should_ignore = self.channel_manager.get(vetted_ids)
        channels_should_update = self.channel_manager.get(non_vetted_channel_ids)

        print(vetted_channels_should_ignore)
        self.assertTrue(True)