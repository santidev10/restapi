import mock
import pickle

from django.utils import timezone
from elasticsearch_dsl import Index

from audit_tool.models import AuditLanguage
from brand_safety.auditors.utils import AuditUtils
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety.auditors.video_auditor import VideoAuditor
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


# Raise OSError to prevent each unit test getting pickled language processors from other tests
@mock.patch.object(pickle, "load", side_effect=OSError)
class BrandSafetyVideoTestCase(ExtendedAPITestCase, ESTestCase):
    SECTIONS = (Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS)
    channel_manager = ChannelManager(sections=SECTIONS)
    video_manager = VideoManager(sections=SECTIONS + (Sections.TASK_US_DATA, Sections.CUSTOM_PROPERTIES,
                                                      Sections.CHANNEL))
    BS_CATEGORIES = ["test"]
    BS_WORDS = ["bad", "word"]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        langs = AuditLanguage.objects.bulk_create([AuditLanguage(language="en"), AuditLanguage(language="ru")])
        bs_category = BadWordCategory.objects.create(name=cls.BS_CATEGORIES[0])
        BadWord.objects.bulk_create([
            BadWord(name=cls.BS_WORDS[0], language=langs[0], category=bs_category),
            BadWord(name=cls.BS_WORDS[1], language=langs[1], category=bs_category),
        ])

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_audit(self, *_):
        """ Test audit word detection """
        video_auditor = VideoAuditor()
        video = Video(f"v_{next(int_iterator)}")
        video.populate_general_data(
            title=self.BS_WORDS[0],
            description=f"{self.BS_WORDS[1]} in description"
        )
        self.video_manager.upsert([video])
        with mock.patch.object(BadWordCategory, "EXCLUDED", return_value=[], new_callable=mock.PropertyMock):
            video_auditor.process([video.main.id])
        Index(Video._index._name).refresh()
        updated = self.video_manager.get([video.main.id])[0]
        all_hits = []
        for category_id in updated.brand_safety.categories:
            hits = [hit["keyword"] for hit in updated.brand_safety.categories[category_id].keywords]
            all_hits.extend(hits)
        self.assertTrue(0 < updated.brand_safety.overall_score < 100)
        self.assertTrue(set(all_hits).issubset(set(self.BS_WORDS)))

    def test_blocklist(self, *_):
        """ Test sets score for blocklisted items """
        video_auditor = VideoAuditor()
        now = timezone.now()
        video = Video(f"v_{next(int_iterator)}")
        video.populate_custom_properties(blocklist=True)
        video.populate_task_us_data(
            last_vetted_at=now,
            brand_safety=[None]
        )
        self.video_manager.upsert([video])
        video_auditor.process([video.main.id])
        updated = self.video_manager.get([video.main.id])[0]
        self.assertEqual(updated.brand_safety.overall_score, 0)

    def test_vetted_safe(self, *_):
        """ Test scoring vetted safe videos should receive all scores of 100 """
        video_auditor = VideoAuditor()
        now = timezone.now()
        video = Video(f"v_{next(int_iterator)}")
        video.populate_task_us_data(
            last_vetted_at=now,
            brand_safety=[None]
        )
        self.video_manager.upsert([video])
        video_auditor.process([video.main.id])
        updated = self.video_manager.get([video.main.id])[0]
        for category in updated.brand_safety.categories:
            self.assertEqual(updated.brand_safety.categories[category].category_score, 100)
        self.assertEqual(updated.brand_safety.overall_score, 100)

    def test_vetted_unsafe(self, *_):
        """ Test scoring vetted unsafe videos should receive all scores of 0 """
        video_auditor = VideoAuditor()
        now = timezone.now()
        video = Video(f"v_{next(int_iterator)}")
        bs_category = BadWordCategory.objects.get(name=self.BS_CATEGORIES[0])
        video.populate_task_us_data(
            last_vetted_at=now,
            brand_safety=[str(bs_category.id)]
        )
        self.video_manager.upsert([video])
        with mock.patch.object(BadWordCategory, "EXCLUDED", return_value=[], new_callable=mock.PropertyMock):
            video_auditor.process([video.main.id])
        updated = self.video_manager.get([video.main.id])[0]
        for category in updated.brand_safety.categories:
            self.assertEqual(updated.brand_safety.categories[category].category_score, 0)
        self.assertEqual(updated.brand_safety.overall_score, 0)

    def test_special_characters(self, *_):
        en_lang = AuditLanguage.objects.get_or_create(language="en")[0]
        sv_lang = AuditLanguage.objects.get_or_create(language="sv")[0]
        bs_category = BadWordCategory.objects.get_or_create(name="test")[0]
        mma_video = Video(**dict(
            main=dict(id=f"channel_{next(int_iterator)}"),
            general_data=dict(description="mma"),
        ))
        swedish_video = Video(**dict(
            main=dict(id=f"channel_{next(int_iterator)}"),
            general_data=dict(description="tycker jagnär man börjar bestämma sig för att man vill anamma den hela"),
        ))
        BadWord.objects.bulk_create([
            BadWord(name="mma", language=en_lang, category=bs_category),
            BadWord(name="mma", language=sv_lang, category=bs_category),
        ])
        audit_utils = AuditUtils()
        english_keywords_processor = audit_utils.bad_word_processors_by_language["en"]
        swedish_keywords_processor = audit_utils.bad_word_processors_by_language["sv"]
        english_hits = english_keywords_processor.extract_keywords(mma_video.general_data.description)
        swedish_hits = swedish_keywords_processor.extract_keywords(swedish_video.general_data.description)
        self.assertEqual(english_hits, ["mma"])
        self.assertEqual(swedish_hits, [])

    def test_audit_serialized(self, *_):
        """ Test audit_serialized method functions properly and without errors """
        bad_words = ", ".join(self.BS_WORDS)
        data = dict(
            id=f"video_{next(int_iterator)}",
            title=bad_words,
            description=bad_words,
            tags=bad_words,
        )
        data2 = dict(
            id=f"video_{next(int_iterator)}",
            title=None,
            description=None,
            tags=None,
        )
        auditor = VideoAuditor()
        with mock.patch.object(BadWordCategory, "EXCLUDED", return_value=[], new_callable=mock.PropertyMock):
            audit = auditor.audit_serialized(data)
            audit2 = auditor.audit_serialized(data2)
        video_audit_score = getattr(audit, "brand_safety_score")
        video_audit_score2 = getattr(audit2, "brand_safety_score")
        self.assertTrue(0 <= video_audit_score.overall_score <= 100)
        self.assertEqual(set(self.BS_WORDS), set(video_audit_score.hits))
        self.assertEqual(video_audit_score2.overall_score, 100)

    def test_ignore_vetted_brand_safety(self, *_):
        """ Test ignore_vetted_brand_safety parameter successfully runs audit without brand safety """
        now = timezone.now()
        video = Video(f"v_{next(int_iterator)}")
        bs_category = BadWordCategory.objects.get(name=self.BS_CATEGORIES[0])
        video.populate_task_us_data(
            last_vetted_at=now,
            brand_safety=[str(bs_category.id)]
        )
        self.video_manager.upsert([video])
        auditor = VideoAuditor(ignore_vetted_brand_safety=True)
        auditor.process([video.main.id])
        updated = self.video_manager.get([video.main.id])[0]
        # ignore_vetted_brand_safety=True should not automatically set scores to 0 because of task_us_data.brand_safety
        self.assertTrue(updated.brand_safety.overall_score != 0)

    def test_channel_rescore(self, *_):
        """ Test that channels are collected if its video scores badly enough """
        video_auditor = VideoAuditor()
        channel = Channel(f"test_channel_id_{next(int_iterator)}")
        channel.populate_brand_safety(overall_score=100)
        video = Video(f"v_{next(int_iterator)}")
        video.populate_general_data(title="test")
        video.populate_channel(id=channel.main.id)
        # Set video score to 0
        video.populate_custom_properties(blocklist=True)

        self.channel_manager.upsert([channel])
        self.video_manager.upsert([video])
        video_auditor.process([video.main.id])
        rescore_channels = video_auditor.channels_to_rescore
        self.assertEqual(set(rescore_channels), {channel.main.id})