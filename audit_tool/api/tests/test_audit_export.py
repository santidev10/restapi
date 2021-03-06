import boto3

from moto import mock_s3

from django.conf import settings
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from audit_tool.api.urls.names import AuditPathName
from audit_tool.api.views.audit_export import AuditExportApiView
from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class AuditExportAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_EXPORT, [Namespace.AUDIT_TOOL])
    mock_s3 = mock_s3()

    def setUp(self):
        self.create_admin_user()
        self.mock_s3.start()
        self.s3 = boto3.resource("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=settings.AMAZON_S3_AUDITS_EXPORTS_BUCKET_NAME)
        video_params = {
            'name': 'test_video',
            'language': 'en'
        }

        channel_params = {
            'name': 'test_channel',
            'language': 'en'
        }

        test_channel = AuditChannel.objects.create(id=2)
        test_video = AuditVideo.objects.create(channel=test_channel, video_id=4, video_id_hash=4)

        video_category = AuditCategory.objects.create(category="Gaming", category_display="Gaming")
        video_language = AuditLanguage.objects.create(language="ru")
        channel_language = AuditLanguage.objects.create(language="en")
        test_country = AuditCountry.objects.create(country="CN")

        AuditVideoMeta.objects.create(video=test_video, name="Dota 2", language=video_language,
                                      category=video_category, views=9000, likes=9999, dislikes=1)
        AuditChannelMeta.objects.create(channel=test_channel, name="Valve",
                                        language=channel_language, view_count=50000,
                                        subscribers=30000, country=test_country, video_count=1
                                        )

        self.video_audit = AuditProcessor.objects.create(audit_type=1, params=video_params, completed=timezone.now())
        self.channel_audit = AuditProcessor.objects.create(audit_type=2, params=channel_params,
                                                           completed=timezone.now())

        AuditVideoProcessor.objects.create(audit=self.video_audit, video=test_video, clean=False,
                                           word_hits={
                                               "exclusion": ["cyka", "blyat", "fuck", "shit"]})
        AuditChannelProcessor.objects.create(audit=self.channel_audit,
                                             channel=test_channel, clean=True,
                                             word_hits={
                                                 "exclusion": ["cyka", "blyat", "fuck", "shit"]
                                             })

    def tearDown(self):
        super().tearDown()
        self.mock_s3.stop()

    def test_video_export(self):
        try:
            video_response = self.client.get(self.url + "?audit_id={}&clean=False".format(self.video_audit.id))
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            raise KeyError("No Audit with id: {} found.".format(self.video_audit.id))
        self.assertEqual(video_response.status_code, HTTP_200_OK)

    def test_channel_export(self):
        try:
            channel_response = self.client.get(self.url + "?audit_id={}&clean=True".format(self.channel_audit.id))
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            raise KeyError("No Audit with id: {} found.".format(self.channel_audit.id))
        self.assertEqual(channel_response.status_code, HTTP_200_OK)

    def test_clean_duration(self):
        clean_duration = AuditExportApiView().clean_duration
        d1 = "PT23M48S"
        cleaned_d1 = clean_duration(d1)
        self.assertEqual(cleaned_d1, "0:23:48")
        d2 = "PT2W5D1H29M3S"
        cleaned_d2 = clean_duration(d2)
        self.assertEqual(cleaned_d2, "457:29:03")
        d3 = "PT1D5H3M18S"
        cleaned_d3 = clean_duration(d3)
        self.assertEqual(cleaned_d3, "29:03:18")
        d4 = "PT1S"
        cleaned_d4 = clean_duration(d4)
        self.assertEqual(cleaned_d4, "0:00:01")
        d5 = "PT1M59S"
        cleaned_d5 = clean_duration(d5)
        self.assertEqual(cleaned_d5, "0:01:59")
        d6 = "PT23H59M59S"
        cleaned_d6 = clean_duration(d6)
        self.assertEqual(cleaned_d6, "23:59:59")
        d7 = "P5W1DT21H34M8S"
        cleaned_d7 = clean_duration(d7)
        self.assertEqual(cleaned_d7, "885:34:08")
        d8 = "P2WT4H24M23S"
        cleaned_d8 = clean_duration(d8)
        self.assertEqual(cleaned_d8, "340:24:23")
