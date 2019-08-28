import types
from unittest.mock import patch

from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from brand_safety.models import BadWordCategory
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase

data = types.SimpleNamespace()
data.brand_safety_score = types.SimpleNamespace()
data.brand_safety_score.overall_score = 100


@patch("brand_safety.auditors.brand_safety_audit.BrandSafetyAudit.manual_video_audit", return_value=[data])
class AuditFlagAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_FLAG, [Namespace.AUDIT_TOOL])

    def setUp(self):
        self.create_admin_user()
        self.flag_category_1 = BadWordCategory.from_string("profanity")
        self.flag_category_2 = BadWordCategory.from_string("porn")
        self.flag_category_3 = BadWordCategory.from_string("violence")

        self.channel_black_list_item = BlacklistItem.objects.create(
            item_type=BlacklistItem.CHANNEL_ITEM,
            item_id="abc",
            item_id_hash=get_hash_name("abc"),
            blacklist_category={self.flag_category_1.id: 100}
        )

        self.video_black_list_item = BlacklistItem.objects.create(
            item_type=BlacklistItem.VIDEO_ITEM,
            item_id="abc",
            item_id_hash=get_hash_name("abc"),
            blacklist_category={self.flag_category_2.id: 100}
        )

    def test_get_existing_channel_blacklist_item(self, manual_video_audit_mock):
        query_url = "{}?item_type={}&item_id={}&flag_categories={}"\
            .format(self.url, self.channel_black_list_item.item_type,
                    self.channel_black_list_item.item_id, self.flag_category_1.id)
        response = self.client.get(query_url)
        self.assertEqual(response.data["BlackListItemDetails"]["item_id"], self.channel_black_list_item.item_id)
        self.assertEqual(response.data["BlackListItemDetails"]["blacklist_category"],
                         {str(self.flag_category_1.id): 100})

    def test_delete_existing_video_blacklist_item(self, manual_video_audit_mock):
        query_url = "{}?item_type={}&item_id={}" \
            .format(self.url, self.video_black_list_item.item_type,
                    self.video_black_list_item.item_id)
        response = self.client.get(query_url)
        self.assertEqual(response.data["BlackListItemDetails"]["item_type"], BlacklistItem.VIDEO_ITEM)
        self.assertEqual(response.data["BlackListItemDetails"]["item_id"], self.video_black_list_item.item_id)
        self.assertEqual(response.data["BlackListItemDetails"]["blacklist_category"],
                         {str(self.flag_category_2.id): 100})
        self.assertEqual(response.data["action"], "BlackListItem deleted.")

    def test_create_new_video_blacklist_item(self, manual_video_audit_mock):
        query_url = "{}?item_type={}&item_id=def&flag_categories=1,2" \
            .format(self.url, BlacklistItem.VIDEO_ITEM)
        response = self.client.get(query_url)
        self.assertEqual(response.data["action"], "BlackListItem created/modified.")
        self.assertEqual(response.data["BlackListItemDetails"]["item_type"], BlacklistItem.VIDEO_ITEM)
        self.assertEqual(response.data["BlackListItemDetails"]["item_id"], "def")
        self.assertEqual(response.data["BlackListItemDetails"]["blacklist_category"],
                         {"1": 100, "2": 100})
        self.assertIsNotNone(response.data.get("overall_score"))
        self.assertEqual(response.data["overall_score"], 100)
