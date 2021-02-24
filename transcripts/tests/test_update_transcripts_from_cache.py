import random
import math

from django.test.testcases import TestCase
from unittest.mock import patch

from audit_tool.constants import AuditVideoTranscriptSourceTypeEnum as SourceTypeEnum
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoTranscript
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from email_reports.tests.test_daily_apex_disney_report_utils import do_nothing
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from transcripts.tasks.update_transcripts_from_cache import TranscriptsFromCacheUpdater
from utils.transform import populate_video_custom_captions


class UpdateTranscriptsFromCacheTestCase(TestCase):

    databases = ["audit", "default"]

    fox_sentence = "the quick brown fox jumps over the lazy dog"
    sphinx_sentence = "Sphinx of black quartz, judge my vow"
    watson_transcript = f"{fox_sentence}. {sphinx_sentence}"
    caption = f"""<?xml version="1.0" encoding="utf-8"?><transcript><text dur="5.14" start="0.74">{fox_sentence}</text>
    <text dur="3.34" start="6.68">{sphinx_sentence}</text></transcript>"""
    video_ids = ["sl71SDyRw28", "2gBXffuUXtA", "fR3wG1H7Rro", "6bkww21HJxc", "rn63Op5dAY4", "WXdtLfN1oMo",
                 "_Hm9vUKhw4c", "fzybvskqx8u", "rdfjxzgh89k", "aenfc2iue64"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY, Sections.TASK_US_DATA),
                                    upsert_sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY))
        self.counts_by_video_id = {}

    def _set_up_data(self):
        audit_videos = []
        #set up AuditVideos
        for video_id in self.video_ids:
            video, _ = AuditVideo.objects.get_or_create(video_id=video_id)
            audit_videos.append(video)
        # set up AuditVideoTranscripts
        english, _ = AuditLanguage.objects.get_or_create(language="en")
        for video in audit_videos:
            # add multiple custom transcripts
            custom_transcripts_count = random.randrange(1, len(TRANSCRIPTS_LANGUAGE_PRIORITY) - 2)
            self.counts_by_video_id[video.video_id] = custom_transcripts_count
            counter = 0
            languages = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()
            random.shuffle(languages)
            for i in range(custom_transcripts_count):
                lang_code = languages.pop()
                language, _ = AuditLanguage.objects.get_or_create(language=lang_code)
                video_transcript, _ = AuditVideoTranscript.objects.get_or_create(video_id=video.id,
                                                                                 transcript=self.caption,
                                                                                 language_id=language.id,
                                                                                 source=SourceTypeEnum.CUSTOM.value)
                counter += 1
                if counter >= custom_transcripts_count:
                    break

            # add a tts_url transcript
            language, _ = AuditLanguage.objects.get_or_create(language=languages.pop())
            tts_url_transcript, _ = AuditVideoTranscript.objects.get_or_create(video_id=video.id,
                                                                               transcript=self.caption,
                                                                               language_id=language.id,
                                                                               source=SourceTypeEnum.TTS_URL.value)

            # add a watson transcript, only supports english
            if "en" in languages:
                watson_transcript, _ = AuditVideoTranscript.objects.get_or_create(video_id=video.id,
                                                                                  transcript=self.watson_transcript,
                                                                                  language_id=english.id,
                                                                                  source=SourceTypeEnum.WATSON.value)

        caption = "asdf"
        language = "en"
        es_videos = []
        for video_id in self.video_ids:
            video = Video(id=video_id)
            populate_video_custom_captions(video, transcript_texts=[caption], transcript_languages=[language],
                                           source="tts_url", asr_lang=language)
            es_videos.append(video)
        self.manager.upsert(es_videos)
        return audit_videos

    @patch("transcripts.tasks.update_transcripts_from_cache.send_email", do_nothing)
    def test_update_success(self):
        audit_videos = self._set_up_data()
        processed_audit_videos = audit_videos[2:]
        unprocessed_audit_videos = audit_videos[:2]
        # get the floor and ceiling of transcript ids to process
        video_ids_to_process = [video.id for video in processed_audit_videos]
        transcript_ids = AuditVideoTranscript.objects.filter(video__id__in=video_ids_to_process).order_by("id") \
            .values_list("id", flat=True)
        transcript_ids = list(transcript_ids)
        with patch.object(TranscriptsFromCacheUpdater, "CHUNK_SIZE", math.floor(len(transcript_ids) / 2)):
            # run the updater on only the processed_audit_videos' transcripts
            updater = TranscriptsFromCacheUpdater()
            updater.run(floor=transcript_ids[0], ceiling=transcript_ids[-1])
            processed_videos = self.manager.get([video.video_id for video in processed_audit_videos])
            for video in processed_videos:
                with self.subTest(video.main.id):
                    # videos should have no more than five custom transcripts
                    custom_transcripts = [item for item in video.custom_captions.items if item.source == "timedtext"]
                    self.assertLessEqual(len(custom_transcripts), 5)
                    pg_transcripts_count = self.counts_by_video_id.get(video.main.id, 0)
                    if pg_transcripts_count < 5:
                        self.assertEqual(len(custom_transcripts), pg_transcripts_count)
                    # videos should have no more than one tts url transcript
                    tts_url_transcripts = [item for item in video.custom_captions.items if item.source == "tts_url"]
                    self.assertEqual(len(tts_url_transcripts), 1)
                    # videos should have no more than one watson transcript
                    watson_transcripts = [item for item in video.custom_captions.items if item.source == "Watson"]
                    self.assertLessEqual(len(watson_transcripts), 1)
                    # ensure that the transcript texts were replaced
                    for transcript in video.custom_captions.items:
                        self.assertNotIn("asdf", transcript.text)
                        self.assertNotIn("xml", transcript.text)
                        self.assertIn(self.fox_sentence, transcript.text)
                        self.assertIn(self.sphinx_sentence, transcript.text)

            unprocessed_videos = self.manager.get([video.video_id for video in unprocessed_audit_videos])
            for video in unprocessed_videos:
                with self.subTest(video.main.id):
                    self.assertEqual(len(video.custom_captions.items), 1)
                    latest_transcript = video.custom_captions.items[-1]
                    self.assertIn("asdf", latest_transcript.text)
                    self.assertNotIn(self.fox_sentence, latest_transcript.text)
                    self.assertNotIn(self.sphinx_sentence, latest_transcript.text)