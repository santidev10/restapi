import time
import mock
import pickle
import random
from random import randint

from django.utils import timezone
from elasticsearch_dsl import Index

from audit_tool.models import AuditLanguage
from brand_safety.auditors.channel_auditor import ChannelAuditor
from brand_safety.auditors.utils import AuditUtils
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
class BrandSafetyTestCase(ExtendedAPITestCase, ESTestCase):
    SECTIONS = (Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.STATS, Sections.TASK_US_DATA, Sections.CUSTOM_PROPERTIES)
    channel_manager = ChannelManager(sections=SECTIONS)
    transcript_manager = TranscriptManager(sections=(Sections.VIDEO, Sections.GENERAL_DATA, Sections.TEXT))
    video_manager = VideoManager(sections=SECTIONS + (Sections.CHANNEL,))
    BS_CATEGORIES = ["channel_test"]
    BS_WORDS = ["channel", "bad", "words"]

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

    def test_special_characters(self, *_):
        en_lang = AuditLanguage.objects.get_or_create(language="en")[0]
        sv_lang = AuditLanguage.objects.get_or_create(language="sv")[0]
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

    def test_transcripts(self, *args, **kwargs):
        """
        test audit of channel videos
        :param args:
        :param kwargs:
        :return:
        """
        channel_id = next(int_iterator)
        channel = Channel(channel_id)
        language_codes = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()
        videos = []
        transcripts = []
        for word in self.BS_WORDS:
            video_id = next(int_iterator)
            video = Video(f"{video_id}")
            video.populate_general_data(description=f"{video_id} desc.")
            video.populate_channel(id=channel_id)
            videos.append(video)
            for _ in range(randint(1, 5)):
                transcript_id = next(int_iterator)
                transcript = Transcript(transcript_id)
                transcript.populate_video(id=video_id)
                transcript.populate_general_data(language_code=random.choice(language_codes))
                transcript.populate_text(value=f"{transcript_id} text the quick brown fox {word}")
                transcripts.append(transcript)
        self.channel_manager.upsert([channel])
        self.video_manager.upsert(videos)
        self.transcript_manager.upsert(transcripts)

        channel_auditor = ChannelAuditor()
        channel_auditor.process([channel.main.id])
        Index(Channel._index._name).refresh()
        updated = self.channel_manager.get([channel.main.id])[0]
        self.assertTrue(0 < updated.brand_safety.overall_score < 100)

    def test_audit_metadata(self, *_):
        """ Test channel audit metadata detection """
        channel_auditor = ChannelAuditor()
        channel = Channel(f"channel_{next(int_iterator)}")
        channel.populate_general_data(
            title=self.BS_WORDS[0],
            description=f"{self.BS_WORDS[1]} in description"
        )
        self.channel_manager.upsert([channel])
        channel_auditor.process([channel.main.id])

        Index(Channel._index._name).refresh()
        updated = self.channel_manager.get([channel.main.id])[0]
        all_hits = []
        for category_id in updated.brand_safety.categories:
            hits = [hit["keyword"] for hit in updated.brand_safety.categories[category_id].keywords]
            all_hits.extend(hits)
        self.assertTrue(0 < updated.brand_safety.overall_score < 100)
        self.assertTrue(set(all_hits).issubset(set(self.BS_WORDS)))

    def test_audit_videos(self, *_):
        channel_auditor = ChannelAuditor()
        """ Test channel videos metadata detection """
        channel = Channel(f"channel_{next(int_iterator)}")
        videos = []
        for word in self.BS_WORDS:
            video = Video(f"v_{next(int_iterator)}")
            video.populate_general_data(description=word)
            video.populate_channel(id=channel.main.id)
            videos.append(video)
        self.channel_manager.upsert([channel])
        self.video_manager.upsert(videos)

        channel_auditor.process(channel.main.id)
        Index(Channel._index._name).refresh()
        Index(Video._index._name).refresh()
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        updated_videos = self.video_manager.get([v.main.id for v in videos])
        all_hits = []
        for category_id in updated_channel.brand_safety.categories:
            hits = [hit["keyword"] for hit in updated_channel.brand_safety.categories[category_id].keywords]
            all_hits.extend(hits)
        self.assertTrue(0 < updated_channel.brand_safety.overall_score < 100)
        self.assertTrue(set(all_hits).issubset(set(self.BS_WORDS)))

        for video in updated_videos:
            curr_video_hits = []
            for category_id in video.brand_safety.categories:
                hits = [hit["keyword"] for hit in video.brand_safety.categories[category_id].keywords]
                curr_video_hits.extend(hits)
                self.assertTrue(0 < updated_channel.brand_safety.overall_score < 100)
                self.assertTrue(set(all_hits).issubset(set(self.BS_WORDS)))

    def test_blocklist(self, *_):
        """ Test sets score for blocklisted items """
        channel_auditor = ChannelAuditor()
        now = timezone.now()
        channel = Channel(f"channel_{next(int_iterator)}")
        channel.populate_custom_properties(blocklist=True)
        channel.populate_task_us_data(
            last_vetted_at=now,
            brand_safety=[None]
        )
        self.channel_manager.upsert([channel])
        channel_auditor.process([channel.main.id])
        updated = self.channel_manager.get([channel.main.id])[0]
        self.assertEqual(updated.brand_safety.overall_score, 0)

    def test_audit_timestamp(self, *_):
        """
        Tests that main.created_at field is not changed after audit
        """
        channel_auditor = ChannelAuditor()
        channel = Channel(f"channel_{next(int_iterator)}")
        self.channel_manager.upsert([channel])
        time.sleep(1)
        channel_auditor.process([channel.main.id])
        audited = self.channel_manager.get([channel.main.id])[0]
        self.assertLess(audited.main.created_at, audited.brand_safety.updated_at)
