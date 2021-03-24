import datetime
from unittest.mock import patch

from googleapiclient.errors import HttpError

from oauth.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.tasks.dv360.sync_dv_records import CREATED_THRESHOLD_MINUTES
from performiq.tasks.dv360.sync_dv_records import sync_dv_partners
from performiq.tasks.dv360.sync_dv_records import UPDATED_THRESHOLD_MINUTES
from utils.unittests.test_case import ExtendedAPITestCase


class DV360SyncTestCase(ExtendedAPITestCase):
    def test_catch_oauth_revoked(self):
        """ Test that account oauth is revoked if api response is invalid"""
        user = self.create_test_user()
        revoked = OAuthAccount.objects.create(
            oauth_type=OAuthType.DV360.value,
            user=user,
            revoked_access=False,
            refresh_token="testrefresh",
            token="testaccess",
        )
        OAuthAccount.objects.filter(id=revoked.id).update(
            created_at=datetime.datetime.now() - datetime.timedelta(minutes=CREATED_THRESHOLD_MINUTES + 1),
            updated_at=datetime.datetime.now() - datetime.timedelta(minutes=UPDATED_THRESHOLD_MINUTES + 1)
        )
        err = HttpError(resp="resp", content="content".encode("utf-8"))
        err.args = [{"status": "403"}]
        with patch("performiq.tasks.dv360.sync_dv_records.request_partners", side_effect=err):
            sync_dv_partners()
        revoked.refresh_from_db()
        self.assertEqual(revoked.revoked_access, True)
