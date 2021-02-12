from django.test.testcases import TestCase
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video
from unittest.mock import patch

from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoTranscript
from email_reports.tests.test_daily_apex_disney_report_utils import do_nothing
from transcripts.tasks.update_transcripts_from_cache import TranscriptsFromCacheUpdater
from utils.transform import populate_video_custom_captions


class UpdateTranscriptsFromCacheTestCase(TestCase):

    databases = ["audit", "default"]

    fox_sentence = "the quick brown fox jumps over the lazy dog"
    sphinx_sentence = "Sphinx of black quartz, judge my vow"
    caption = f"""<?xml version="1.0" encoding="utf-8"?><transcript><text dur="5.14" start="0.74">{fox_sentence}</text>
    <text dur="3.34" start="6.68">{sphinx_sentence}</text></transcript>"""
    video_ids = ["sl71SDyRw28", "2gBXffuUXtA", "fR3wG1H7Rro", "6bkww21HJxc", "rn63Op5dAY4", "WXdtLfN1oMo",
                 "_Hm9vUKhw4c", "fzybvskqx8u", "rdfjxzgh89k", "aenfc2iue64"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manager = VideoManager(sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY, Sections.TASK_US_DATA),
                                    upsert_sections=(Sections.CUSTOM_CAPTIONS, Sections.BRAND_SAFETY))

    def _set_up_data(self):
        audit_videos = []
        for video_id in self.video_ids:
            video, _ = AuditVideo.objects.get_or_create(video_id=video_id)
            audit_videos.append(video)
        for video in audit_videos:
            video_transcript, _ = AuditVideoTranscript.objects.get_or_create(video_id=video.id, transcript=self.caption)

        caption = "asdf"
        language = "en"
        es_videos = []
        for video_id in self.video_ids:
            video = Video(id=video_id)
            populate_video_custom_captions(video, transcript_texts=[caption], transcript_languages=[language],
                                           source="tts_url", asr_lang=language)
            es_videos.append(video)
        self.manager.upsert(es_videos)

    @patch("transcripts.tasks.update_transcripts_from_cache.send_email", do_nothing)
    def test_update_success(self):
        self._set_up_data()
        with patch.object(TranscriptsFromCacheUpdater, "CHUNK_SIZE", 5):
            second_item = AuditVideoTranscript.objects.order_by("id").first().id + 1
            second_to_last_item = AuditVideoTranscript.objects.order_by("id").last().id - 1
            updater = TranscriptsFromCacheUpdater()
            updater.run(floor=second_item, ceiling=second_to_last_item)
            processed_videos = self.manager.get(self.video_ids[1:9])
            for video in processed_videos:
                with self.subTest(video.main.id):
                    latest_transcript = video.custom_captions.items[-1]
                    self.assertNotIn("asdf", latest_transcript.text)
                    self.assertNotIn("xml", latest_transcript.text)
                    self.assertIn(self.fox_sentence, latest_transcript.text)
                    self.assertIn(self.sphinx_sentence, latest_transcript.text)

            unprocessed_videos = self.manager.get(self.video_ids[:0] + self.video_ids[-1:])
            for video in unprocessed_videos:
                latest_transcript = video.custom_captions.items[-1]
                self.assertIn("asdf", latest_transcript.text)
                self.assertNotIn(self.fox_sentence, latest_transcript.text)
                self.assertNotIn(self.sphinx_sentence, latest_transcript.text)