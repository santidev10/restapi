from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from performiq.api.urls.names import PerformIQPathName
from performiq.models.constants import OAuthType
from performiq.models import Campaign
from performiq.models import IQCampaign
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase


class PerformIQAnalysisRetrieveTestCase(ExtendedAPITestCase):
    def _get_url(self, pk):
        return reverse(Namespace.PERFORMIQ + ":" + PerformIQPathName.CAMPAIGN, kwargs=dict(pk=pk))

    def setUp(self) -> None:
        self.user = self.create_test_user(perms={
            StaticPermissions.PERFORMIQ: True
        })

    def test_not_found(self):
        response = self.client.get(self._get_url(1))
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_permission_fail(self):
        """ Test scenario where user previously had access but was revoked """
        self.user.perms = {}
        self.user.save()
        iq = IQCampaign.objects.create(user=self.user)
        response = self.client.get(self._get_url(iq.id))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_non_owner_fail(self):
        """ Test users are not allowed to view other analyses """
        user1 = get_user_model().objects.create(email="test1")
        iq = IQCampaign.objects.create(user=user1)
        response = self.client.get(self._get_url(iq.id))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_owner_success(self):
        campaign = Campaign.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value)
        iq = IQCampaign.objects.create(user=self.user, campaign=campaign)
        response = self.client.get(self._get_url(iq.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
