import json

from django.contrib.auth.models import Group

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import SegmentChannel
from userprofile.permissions import PermissionGroupNames, Permissions
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.reverse import reverse


class SegmentListCreateApiViewTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type, segment_pk):
        return reverse(Name.SEGMENT_SHARE, [Namespace.SEGMENT], kwargs=dict(segment_type=segment_type, pk=segment_pk))

    def test_access_for_collaborator(self):
        collaborator_email = "collaborator@example.com"
        collaborator = self.create_test_user(**{"email": collaborator_email})
        user = self.create_test_user()
        owned_segment = SegmentChannel.objects.create(owner=user)
        data = json.dumps({"shared_with": [collaborator_email]})
        Permissions.sync_groups()
        self.client.put(self._get_url("channel", owned_segment.id), data=data, content_type="application/json")
        self.assertIn(Group.objects.get(name=PermissionGroupNames.MEDIA_PLANNING), collaborator.groups.all())
