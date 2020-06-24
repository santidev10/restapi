from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.models import AuditLanguage
from audit_tool.models import AuditVideoTranscript
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from transcripts.api.urls.names import TranscriptsPathName
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class WatsonTranscriptsPostTestCase(ExtendedAPITestCase, ESTestCase):
    url = reverse(TranscriptsPathName.WATSON_TRANSCRIPTS, [Namespace.TRANSCRIPTS])

    @classmethod
    def setUpClass(cls):
        super(WatsonTranscriptsPostTestCase, cls).setUpClass()

    def setUp(self):
        self.vid_id_1 = str(next(int_iterator))
        self.vid_id_2 = str(next(int_iterator))
        sections = [Sections.CUSTOM_CAPTIONS, Sections.GENERAL_DATA]
        video_1 = Video(**{
            "meta": {
                "id": self.vid_id_1
            },
            "main": {
                "id": self.vid_id_1
            }
        })
        video_2 = Video(**{
            "meta": {
                "id": self.vid_id_2
            },
            "main": {
                "id": self.vid_id_2
            },
            "general_data": {
                "language": "Spanish",
                "lang_code": "es"
            }
        })
        VideoManager(sections=sections).upsert([video_1, video_2])
        AuditVideoTranscript.get_or_create(video_id=self.vid_id_1, language="en", transcript=None, source=1)
        AuditVideoTranscript.get_or_create(video_id=self.vid_id_2, language="es", transcript=None, source=1)

    def test_authorized_transcript_post(self):
        url = f"{self.url}?authorization=f013fce59e6eecb09c19706f04da906173f5bc1d"
        response = self.client.post(url, dict())
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, "Stored transcripts for 0 videos: ")

    def test_unauthorized_transcript_post(self):
        url = f"{self.url}?authorization=iamahacker"
        response = self.client.post(url, dict())
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(response.data, "Invalid authorization token for POST /transcripts.")

    def test_transcript_post_update(self):
        url = f"{self.url}?authorization=f013fce59e6eecb09c19706f04da906173f5bc1d"

        request_body = {
            self.vid_id_1: {
                "transcript": "Test Transcript One"
            },
            self.vid_id_2: {
                "transcript": "Test Transcript Two"
            }
        }
        response = self.client.post(url, data=request_body, format="json")
        audit_transcript_1 = AuditVideoTranscript.get_or_create(video_id=self.vid_id_1, language="en", source=1)
        audit_transcript_2 = AuditVideoTranscript.get_or_create(video_id=self.vid_id_2, language="es", source=1)
        self.assertEqual(audit_transcript_1.language, AuditLanguage.from_string("en"))
        self.assertEqual(audit_transcript_1.transcript, "Test Transcript One")
        self.assertEqual(audit_transcript_2.language, AuditLanguage.from_string("es"))
        self.assertEqual(audit_transcript_2.transcript, "Test Transcript Two")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, f"Stored transcripts for 2 videos: {self.vid_id_1}, {self.vid_id_2}")
