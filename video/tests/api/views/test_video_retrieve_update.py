# pylint: disable=cyclic-import
import random
from random import randint
from random import shuffle
from unittest.mock import patch

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from es_components.constants import Sections
from es_components.managers import TranscriptManager
from es_components.managers import VideoManager
from es_components.models.transcript import Transcript
from es_components.models.video import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from transcripts.constants import TranscriptSourceTypeEnum
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase
from video.api.urls.names import Name


class VideoRetrieveUpdateTestSpec(ExtendedAPITestCase, ESTestCase):

    def _get_url(self, video_id):
        return reverse(
            Name.VIDEO,
            [Namespace.VIDEO],
            args=(video_id,),
        )

    @patch("brand_safety.auditors.utils.AuditUtils.get_items", return_value=[])
    def test_professional_user_should_see_video_aw_data(self, mock_get_items):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        mock_get_items.return_value = []
        user = self.create_test_user(auth=True, perms={
            StaticPermissions.RESEARCH__CHANNEL_VIDEO_DATA: True,
        })

        video_id = "video_id"

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=Video(id=video_id, ads_stats={"clicks_count": 100})):
            url = self._get_url(video_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIn("ads_stats", response.data)

    @patch("brand_safety.auditors.utils.AuditUtils.get_items", return_value=[])
    def test_user_should_see_chart_data(self, mock_get_items):
        mock_get_items.return_value = []
        user = self.create_test_user(auth=True, perms={
            StaticPermissions.RESEARCH__CHANNEL_VIDEO_DATA: True,
        })

        video_id = "video_id"

        stats = {
            "likes_raw_history": {
                "2020-01-02": 1003,
                "2020-01-09": 1500
            },
            "dislikes_raw_history": {
                "2020-01-02": 300,
                "2020-01-09": 780
            },
            "comments_raw_history": {
                "2020-01-02": 300,
                "2020-01-09": 780
            },
            "views_raw_history": {
                "2020-01-02": 10300,
                "2020-01-09": 30300,
            }
        }

        expected_data = [
            {
                "created_at": "2020-01-02 23:59:59.999999Z",
                "views": 10300,
                "likes": 1003,
                "dislikes": 300,
                "comments": 300
            },
            {
                "created_at": "2020-01-09 23:59:59.999999Z",
                "views": 30300,
                "likes": 1500,
                "dislikes": 780,
                "comments": 780
            },
        ]

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=Video(id=video_id, stats=stats)):
            url = self._get_url(video_id)
            response = self.client.get(url)

            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(response.data.get("chart_data"), expected_data)

    def test_404_if_no_video(self):
        self.create_test_user(perms={
            StaticPermissions.RESEARCH__CHANNEL_VIDEO_DATA: True,
        })
        missing_video_id = "some_id"

        with patch("es_components.managers.video.VideoManager.model.get",
                   return_value=None):
            url = self._get_url(missing_video_id)
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "transcript", "blacklist_data")
        video = Video(str(next(int_iterator)))
        VideoManager([Sections.GENERAL_DATA]).upsert([video])

        url = self._get_url(video.main.id)
        response = self.client.get(url)

        for field in extra_fields:
            with self.subTest(field):
                self.assertIn(field, response.data)

    def test_mapped_fields(self):
        """
        test that fields mapped from other fields are present and correct
        :return:
        """
        self.create_admin_user()
        video = Video(str(next(int_iterator)))
        video.populate_general_data(lang_code="en")
        VideoManager([Sections.GENERAL_DATA]).upsert([video])

        url = self._get_url(video.main.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get(Sections.GENERAL_DATA, {}).get("language"), "English")

    def test_transcripts(self):
        """
        test that transcripts are retrieved from the new index, and fallback to the old video caption_items records
        :return:
        """
        video_id = next(int_iterator)
        # new transcripts even IDs, old transcripts odd IDs
        video = Video(video_id)
        languages = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()
        video_language = random.choice(languages)
        video.populate_general_data(
            title=f"{video_id} title",
            description=f"{video_id} desc.",
            lang_code=video_language
        )
        transcript_text = f"correct transcript for video with language: {video_language}"
        shuffle(languages)
        transcripts_count = randint(2, 5)
        transcript_languages = [video_language] + languages[:transcripts_count - 1]

        old_transcripts = []
        for language in transcript_languages:
            transcript = dict(
                text="incorrect transcript",
                language_code=language,
                source=TranscriptSourceTypeEnum.TTS_URL.value,
                is_asr=True
            )
            old_transcripts.append(transcript)
        video.populate_custom_captions(items=old_transcripts)
        video_manager = VideoManager(sections=[Sections.GENERAL_DATA, Sections.CUSTOM_CAPTIONS])
        video_manager.upsert([video])

        new_transcripts = []
        for language in transcript_languages:
            transcript_id = next(int_iterator)
            transcript = Transcript(transcript_id)
            transcript.populate_video(id=video_id)
            transcript.populate_general_data(language_code=language)
            transcript.populate_text(value=transcript_text if language == video_language else "asdf")
            new_transcripts.append(transcript)

        transcript_manager = TranscriptManager(sections=[Sections.GENERAL_DATA, Sections.VIDEO, Sections.TEXT])
        transcript_manager.upsert(new_transcripts)

        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
            StaticPermissions.RESEARCH__CHANNEL_VIDEO_DATA: True,
        })
        url = self._get_url(video.main.id)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        res_video_id = data.get("main", {}).get("id")
        self.assertEqual(res_video_id, video_id)
        lang_code = data.get("general_data", {}).get("lang_code")
        self.assertTrue(lang_code)
        transcript = data.get("transcript")
        self.assertTrue(isinstance(transcript, str))
        self.assertEqual(transcript, f"correct transcript for video with language: {lang_code}")
