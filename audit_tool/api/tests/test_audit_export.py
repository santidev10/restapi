from tempfile import mkstemp
import csv
from rest_framework.status import HTTP_200_OK

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
from audit_tool.api.views import AuditExportApiView

from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase

class AuditSaveAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_EXPORT, [Namespace.AUDIT_TOOL])

    def setUp(self):
        self.create_admin_user()
        video_params = {
            'name': 'test_video',
            'language': 'en'
        }

        channel_params = {
            'name': 'test_channel',
            'language': 'en'
        }

        test_channel = AuditChannel.objects.create(id=2)
        test_video = AuditVideo.objects.create(channel=test_channel, video_id=4)

        video_category = AuditCategory(category="Gaming", category_display="Gaming")
        video_language = AuditLanguage(language="ru")
        channel_language = AuditLanguage(language="en")
        test_country = AuditCountry(country="CN")

        test_video_meta = AuditVideoMeta.objects.create(video=test_video, name="Dota 2", language=video_language,
                                                        category=video_category, views=9000, likes=9999, dislikes=1)
        test_channel_meta = AuditChannelMeta.objects.create(channel=test_channel, name="Valve",
                                                            language=channel_language, view_count=50000,
                                                            subscribers=30000, country=test_country
                                                            )

        video_audit = AuditProcessor.objects.create(audit_type=1, params=video_params, completed=True)
        channel_audit = AuditProcessor.objects.create(audit_type=2, params=channel_params, completed=True)

        video_processor = AuditVideoProcessor.objects.create(audit=video_audit, )
        channel_processor = AuditChannelProcessor.objects.create(audit=channel_audit, )
