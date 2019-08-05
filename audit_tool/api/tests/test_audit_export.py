import csv
from rest_framework.status import HTTP_200_OK
from django.utils import timezone

from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditChannel
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditCategory
from audit_tool.models import AuditCountry
from audit_tool.api.urls.names import AuditPathName
from audit_tool.api.views.audit_export import AuditS3Exporter
from audit_tool.api.views.audit_export import AuditExportApiView

from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class AuditExportAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_EXPORT, [Namespace.AUDIT_TOOL])

    def setUp(self):
        self.create_admin_user()
        self.s3 = AuditS3Exporter._s3()
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

        test_video_meta = AuditVideoMeta.objects.create(video=test_video, name="Dota 2", language=video_language,
                                                        category=video_category, views=9000, likes=9999, dislikes=1)
        test_channel_meta = AuditChannelMeta.objects.create(channel=test_channel, name="Valve",
                                                            language=channel_language, view_count=50000,
                                                            subscribers=30000, country=test_country, video_count=1
                                                            )

        self.video_audit = AuditProcessor.objects.create(audit_type=1, params=video_params, completed=timezone.now())
        self.channel_audit = AuditProcessor.objects.create(audit_type=2, params=channel_params, completed=timezone.now())

        video_processor = AuditVideoProcessor.objects.create(audit=self.video_audit, video=test_video, clean=False,
                                                             word_hits={"exclusion": ["cyka", "blyat", "fuck", "shit"]})
        channel_processor = AuditChannelProcessor.objects.create(audit=self.channel_audit,
                                                                 channel=test_channel, clean=True,
                                                                 word_hits={
                                                                     "exclusion": ["cyka", "blyat", "fuck", "shit"]
                                                                 })

    def tearDown(self):
        video_audit_key = 'export_{}_{}_false.csv'.format(self.video_audit.id, self.video_audit.params['name'])
        try:
            self.s3.delete_object(
                Bucket=AuditS3Exporter.bucket_name,
                Key=video_audit_key
            )
        except Exception as e:
            raise KeyError("Failed to delete object. Object with key {} not found in bucket."
                           .format(video_audit_key))

        channel_audit_key = 'export_{}_{}_true.csv'.format(self.channel_audit.id, self.channel_audit.params['name'])
        try:
            self.s3.delete_object(
                Bucket=AuditS3Exporter.bucket_name,
                Key=channel_audit_key
            )
        except Exception as e:
            raise KeyError(
                "Failed to delete object. Object with key {} not found in bucket."
                    .format(channel_audit_key))


    def test_video_export(self):
        try:
            video_response = self.client.get(self.url + "?audit_id={}&clean=False".format(self.video_audit.id))
        except Exception as e:
            raise KeyError("No Audit with id: {} found.".format(self.video_audit.id))
        self.assertEqual(video_response.status_code, HTTP_200_OK)


    def test_channel_export(self):
        try:
            channel_response = self.client.get(self.url + "?audit_id={}&clean=True".format(self.channel_audit.id))
        except Exception as e:
            raise KeyError("No Audit with id: {} found.".format(self.channel_audit.id))
        self.assertEqual(channel_response.status_code, HTTP_200_OK)


    def test_clean_duration(self):
        clean_duration = AuditExportApiView().clean_duration
        d1 = "PT23M48S"
        cleaned_d1 = clean_duration(d1)
        self.assertEqual(cleaned_d1, "0:23:48")
        d2 = "PT2W5D1H29M3S"
        cleaned_d2 = clean_duration(d2)
        self.assertEqual(cleaned_d2, "19 days, 1:29:03")
        d3 = "PT1D5H3M18S"
        cleaned_d3 = clean_duration(d3)
        self.assertEqual(cleaned_d3, "1 day, 5:03:18")
        d4 = "PT1S"
        cleaned_d4 = clean_duration(d4)
        self.assertEqual(cleaned_d4, "0:00:01")
        d5 = "PT1M59S"
        cleaned_d5 = clean_duration(d5)
        self.assertEqual(cleaned_d5, "0:01:59")
        d6 = "PT23H59M59S"
        cleaned_d6 = clean_duration(d6)
        self.assertEqual(cleaned_d6, "23:59:59")
