import json
from unittest import mock

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from oauth.constants import OAuthType
from oauth.models import AdGroup
from oauth.models import DV360Partner
from oauth.models import DV360Advertiser
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.constants import SegmentTypeEnum
from utils.unittests.test_case import ExtendedAPITestCase


class CTLDV360SyncTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_id):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_SYNC_DV360, kwargs=dict(pk=segment_id))

    def setUp(self) -> None:
        self.user = self.create_test_user()

    def _mock_data(self):
        segment = CustomSegment.objects.create(owner=self.user, segment_type=int(SegmentTypeEnum.CHANNEL))
        partner = DV360Partner.objects.create(id=1)
        advertiser = DV360Advertiser.objects.create(id=1, partner=partner)
        adgroups = [AdGroup.objects.create(id=i, oauth_type=int(OAuthType.DV360)) for i in range(2)]
        return segment, advertiser, adgroups

    def test_invalid_ctl(self):
        response = self.client.post(self._get_url(None))
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_invalid_advertiser(self):
        segment, advertiser, adgroups = self._mock_data()
        payload = json.dumps(dict(advertiser_id=-1, adgroup_ids=[adgroups[0].id]))
        response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_invalid_adgroups(self):
        segment, advertiser, adgroups = self._mock_data()
        with self.subTest("None provided"):
            payload = json.dumps(dict(advertiser_id=advertiser.id, adgroup_ids=[]))
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.subTest("Some invalid"):
            payload = json.dumps(dict(advertiser_id=advertiser.id, adgroup_ids=[adgroups[0].id, -1]))
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.subTest("All invalid"):
            payload = json.dumps(dict(advertiser_id=advertiser.id, adgroup_ids=[-2, -1]))
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success(self):
        segment, advertiser, adgroups = self._mock_data()
        ag_ids = [ag.id for ag in adgroups]
        payload = json.dumps(dict(
            advertiser_id=advertiser.id,
            adgroup_ids=ag_ids,
        ))
        with mock.patch("segment.api.views.custom_segment.segment_dv360_sync.SegmentDV360SyncAPIView._start_audit") as mock_start_audit:
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        segment.refresh_from_db()
        mock_start_audit.assert_called_once()
        dv360_params = segment.params[Params.DV360_SYNC_DATA]
        self.assertEqual(dv360_params[Params.ADVERTISER_ID], advertiser.id)
        self.assertEqual(dv360_params[Params.ADGROUP_IDS], ag_ids)
