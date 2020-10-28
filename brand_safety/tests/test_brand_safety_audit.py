from datetime import timedelta

from django.utils import timezone
from mock import PropertyMock
from mock import patch

from audit_tool.models import AuditLanguage
from brand_safety.auditors.utils import AuditUtils
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety.tasks.channel_outdated import channel_outdated_scheduler
from brand_safety.tasks.constants import Schedulers
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class BrandSafetyTestCase(ExtendedAPITestCase, ESTestCase):
    vetted_channel_manager = ChannelManager(
        sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS, Sections.TASK_US_DATA))
    channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS))
    vetted_video_manager = VideoManager(
        sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS, Sections.TASK_US_DATA))
    video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS))

    def setup_data(self):
        langs = AuditLanguage.objects.bulk_create([AuditLanguage(language="en"), AuditLanguage(language="ru")])
        bs_category = BadWordCategory.objects.create(name="test")
        bad_words = BadWord.objects.bulk_create([
            BadWord(name="bad", language=langs[0], category=bs_category),
            BadWord(name="word", language=langs[1], category=bs_category),
        ])
        outdated_time = timezone.now() - timedelta(days=100)
        now = timezone.now()
        bs_id = str(bs_category.id)
        vetted_channels = [
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=100)}, updated_at=outdated_time),
                task_us_data=dict(brand_safety=[bs_id]),
                general_data=dict(description="A bad word", updated_at=now),
                stats=dict(total_videos_count=1),
            )),
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=0)}, updated_at=outdated_time),
                task_us_data=dict(brand_safety=[bs_id]),
                general_data=dict(description="Another bad word", updated_at=now),
                stats=dict(total_videos_count=1),
            )),
        ]
        non_vetted_channels = [
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=0)}, updated_at=outdated_time),
                general_data=dict(description="Another bad word", updated_at=now),
                stats=dict(total_videos_count=1),
            )),
            Channel(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                brand_safety=dict(categories={bs_id: dict(category_score=0)}, updated_at=outdated_time),
                general_data=dict(description="A bad word", updated_at=now),
                stats=dict(total_videos_count=1),
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
        # Upsert with different sections to avoid automatic task_us_data section with timestamp creation
        self.channel_manager.upsert(non_vetted_channels)
        self.vetted_channel_manager.upsert(vetted_channels)

        self.video_manager.upsert(non_vetted_videos)
        self.vetted_video_manager.upsert(vetted_videos)
        return vetted_channels, non_vetted_channels, vetted_videos, non_vetted_videos, bad_words, outdated_time

    def test_brand_safety(self):
        """ Test skip scoring vetted items """
        vetted_channels, non_vetted_channels, vetted_videos, non_vetted_videos, *_ = self.setup_data()
        vetted_channel_ids = [item.main.id for item in vetted_channels]
        non_vetted_channel_ids = [item.main.id for item in non_vetted_channels]

        vetted_video_ids = [item.main.id for item in vetted_videos]
        non_vetted_video_ids = [item.main.id for item in non_vetted_videos]
        with patch.object(Schedulers.ChannelOutdated, "get_items_limit", return_value=10), \
             patch.object(Schedulers.ChannelOutdated, "UPDATE_TIME_THRESHOLD", return_value="now",
                          new_callable=PropertyMock), \
             patch("brand_safety.tasks.channel_update_helper.get_queue_size", return_value=0),\
             patch("brand_safety.tasks.channel_outdated.get_queue_size", return_value=0): \
            channel_outdated_scheduler()

        vetted_channels_should_ignore = self.vetted_channel_manager.get(vetted_channel_ids)
        channels_should_update = self.vetted_channel_manager.get(non_vetted_channel_ids)

        # vetted documents should not have brand safety scored
        for vetted, ignored in zip(vetted_channels, vetted_channels_should_ignore):
            self.assertEqual(vetted.brand_safety, ignored.brand_safety)

        # Non vetted documents should have brand safety mutated / scored
        for non_vetted, scored in zip(non_vetted_channels, channels_should_update):
            self.assertNotEqual(non_vetted.brand_safety, scored.brand_safety)

        vetted_videos_should_ignore = self.vetted_video_manager.get(vetted_video_ids)
        videos_should_update = self.vetted_video_manager.get(non_vetted_video_ids)

        # Vetted videos should not be scored, even if under non vetted channel
        for vetted, ignored in zip(vetted_videos, vetted_videos_should_ignore):
            self.assertEqual(vetted.brand_safety, ignored.brand_safety)

        for non_vetted, scored in zip(non_vetted_videos, videos_should_update):
            self.assertNotEqual(non_vetted.brand_safety, scored.brand_safety)

    def test_special_characters(self):
        langs = AuditLanguage.objects.bulk_create([AuditLanguage(language="en"), AuditLanguage(language="sv")])
        bs_category = BadWordCategory.objects.create(name="test")
        mma_video = Video(**dict(
            main=dict(id=f"channel_{next(int_iterator)}"),
            general_data=dict(description="mma"),
        ))
        swedish_video = Video(**dict(
            main=dict(id=f"channel_{next(int_iterator)}"),
            general_data=dict(description="tycker jagnär man börjar bestämma sig för att man vill anamma den hela"),
        ))
        BadWord.objects.bulk_create([
            BadWord(name="mma", language=langs[0], category=bs_category),
            BadWord(name="mma", language=langs[1], category=bs_category),
        ])
        with patch("brand_safety.auditors.utils.pickle.load", side_effect=OSError):
            audit_utils = AuditUtils()
        english_keywords_processor = audit_utils.bad_word_processors_by_language["en"]
        swedish_keywords_processor = audit_utils.bad_word_processors_by_language["sv"]
        english_hits = english_keywords_processor.extract_keywords(mma_video.general_data.description)
        swedish_hits = swedish_keywords_processor.extract_keywords(swedish_video.general_data.description)
        self.assertEqual(english_hits, ["mma"])
        self.assertEqual(swedish_hits, [])
