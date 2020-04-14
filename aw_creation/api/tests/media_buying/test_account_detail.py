from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase

from aw_creation.api.tests.media_buying.test_media_buying_mixin import TestMediaBuyingMixin


class AccountMediaBuyingDetailTestCase(ExtendedAPITestCase, TestMediaBuyingMixin):
    def _get_url(self, pk):
        url = reverse(pk=pk)

    def test_get_detail_success(self):
        user = self.create_admin_user()
        account = self.create_account()

