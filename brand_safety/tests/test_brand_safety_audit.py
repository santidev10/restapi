from datetime import timedelta

from django.utils import timezone
from mock import patch

from audit_tool.models import AuditLanguage
from brand_safety.auditors.utils import AuditUtils
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class BrandSafetyTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS))
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
        channels = [
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
        videos = [
            Video(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                channel=dict(id=channels[0].main.id),
                general_data=dict(description="Another bad word"),
            )),
            Video(**dict(
                main=dict(id=f"channel_{next(int_iterator)}"),
                channel=dict(id=channels[1].main.id),
                general_data=dict(description="A bad word"),
            )),
        ]
        self.channel_manager.upsert(channels)
        self.video_manager.upsert(videos)
        return channels, videos, bad_words, outdated_time

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
