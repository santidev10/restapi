import pickle
import random
import time
from unittest import mock
from random import randint

from django.utils import timezone
from elasticsearch_dsl import Index

from audit_tool.models import AuditLanguage
from brand_safety.auditors.utils import AuditUtils
from brand_safety.auditors.utils import remove_tags_punctuation
from brand_safety.auditors.video_auditor import VideoAuditor
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import TranscriptManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Transcript
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


# Raise OSError to prevent each unit test getting pickled language processors from other tests
@mock.patch.object(pickle, "load", side_effect=OSError)
@mock.patch.object(BadWordCategory, "EXCLUDED", return_value=[], new_callable=mock.PropertyMock)
class BrandSafetyVideoTestCase(ExtendedAPITestCase, ESTestCase):
    SECTIONS = (Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS)
    channel_manager = ChannelManager(sections=SECTIONS)
    transcript_manager = TranscriptManager(sections=(Sections.VIDEO, Sections.GENERAL_DATA, Sections.TEXT))
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

    def test_transcript(self, *args, **kwargs):
        """
        test scoring on transcripts from the new transcripts index
        :param args:
        :param kwargs:
        :return:
        """
        videos = []
        transcripts = []
        language_codes = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()
        for _ in range(5):
            video_id = f"{next(int_iterator)}"
            video = Video(video_id)
            video.populate_general_data(
                title=f"{video_id} title",
                description=f"{video_id} desc."
            )
            videos.append(video)
            for _ in range(randint(1, 5)):
                transcript_id = next(int_iterator)
                transcript = Transcript(transcript_id)
                transcript.populate_video(id=video_id)
                transcript.populate_general_data(language_code=random.choice(language_codes))
                transcript.populate_text(value=f"{transcript_id} text the quick brown fox {self.BS_WORDS[0]}")
                transcripts.append(transcript)

        self.video_manager.upsert(videos)
        self.transcript_manager.upsert(transcripts)
        video_auditor = VideoAuditor()
        video_ids = [video.main.id for video in videos]
        video_auditor.process(video_ids)

        updated_videos = self.video_manager.get(video_ids)
        for video in updated_videos:
            with self.subTest(video):
                self.assertTrue(0 < video.brand_safety.overall_score < 100)

    def test_audit(self, *_):
        """ Test audit word detection """
        video_auditor = VideoAuditor()
        video = Video(f"v_{next(int_iterator)}")
        video.populate_general_data(
            title=self.BS_WORDS[0],
            description=f"{self.BS_WORDS[1]} in description"
        )
        self.video_manager.upsert([video])
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
            general_data=dict(description="tycker jagn??r man b??rjar best??mma sig f??r att man vill anamma den hela"),
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

    def test_punctuation_characters(self, *_):
        en_lang = AuditLanguage.objects.get_or_create(language="en")[0]
        bs_category = BadWordCategory.objects.get_or_create(name="test")[0]
        mma_video = Video(**dict(
            main=dict(id=f"channel_{next(int_iterator)}"),
            general_data=dict(description="test!\"%&\'()+,-./:;<=>?[\\]^_`{|}~mma test@test$test#test*test"),
        ))
        BadWord.objects.bulk_create([
            BadWord(name="mma", language=en_lang, category=bs_category)
        ])
        audit_utils = AuditUtils()
        english_keywords_processor = audit_utils.bad_word_processors_by_language["en"]
        english_video_description = remove_tags_punctuation(mma_video.general_data.description)
        english_hits = english_keywords_processor.extract_keywords(english_video_description)
        self.assertEqual(english_video_description, "test                            mma test@test$test#test*test")
        self.assertEqual(english_hits, ["mma"])

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
        with mock.patch.object(VideoManager, "get") as mock_get,\
                mock.patch.object(VideoManager, "search") as mock_search,\
                mock.patch.object(VideoManager, "upsert") as mock_upsert:
            audit = auditor.audit_serialized(data)
            audit2 = auditor.audit_serialized(data2)

            mock_get.assert_not_called()
            mock_search.assert_not_called()
            mock_upsert.assert_not_called()
        video_audit_score = getattr(audit, "brand_safety_score")
        video_audit_score2 = getattr(audit2, "brand_safety_score")
        self.assertTrue(0 <= video_audit_score.overall_score <= 100)
        self.assertEqual(set(self.BS_WORDS), set(video_audit_score.hits))
        self.assertEqual(video_audit_score2.overall_score, 100)

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

    def test_audit_timestamp(self, *_):
        """
        Tests that main.created_at field is not changed after audit
        """
        video_auditor = VideoAuditor()
        video = Video(f"video_{next(int_iterator)}")
        self.video_manager.upsert([video])
        time.sleep(1)
        video_auditor.process([video.main.id])
        audited = self.video_manager.get([video.main.id])[0]
        self.assertLess(audited.main.created_at, audited.brand_safety.updated_at)
