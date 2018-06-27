from contextlib import contextmanager
from datetime import datetime, date
from unittest.mock import patch

from django.core.management import call_command
from django.test import TransactionTestCase
from pytz import timezone

from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import Account, AWConnectionToUserRelation, \
    AWConnection, AWAccountPermission, Campaign, AdGroup
from segment.models import SegmentChannel, SegmentRelatedChannel, \
    SegmentVideo, SegmentKeyword, SegmentRelatedVideo, SegmentRelatedKeyword
from userprofile.models import UserProfile
from utils.utils_tests import \
    SingleDatabaseApiConnectorPatcher as ConnectionPatch, patch_now, \
    generic_test_method, generic_test_case


@contextmanager
def patch_sdb():
    with patch("segment.models.video.Connector", new=ConnectionPatch), \
         patch("segment.models.keyword.Connector", new=ConnectionPatch), \
         patch("segment.models.channel.Connector", new=ConnectionPatch), \
         patch("segment.models.base.Connector", new=ConnectionPatch):
        yield


EMPTY_STATS = {key: None for key in
               ("average_cpv", "ctr", "ctr_v", "video_view_rate")}

related_classes = {
    SegmentChannel: SegmentRelatedChannel,
    SegmentVideo: SegmentRelatedVideo,
    SegmentKeyword: SegmentRelatedKeyword,
}


def get_related_ref(segment_class):
    return "keyword" \
        if segment_class is SegmentKeyword \
        else "yt_id"


@generic_test_case
class UpdateSegmentsTestCase(TransactionTestCase):
    generic_args_list = [
        ("channel_segment", (SegmentChannel,), dict()),
        ("video_segment", (SegmentVideo,), dict()),
        ("keyword_segment", (SegmentKeyword,), dict()),
    ]

    def setUp(self):
        chf_account_id = load_web_app_settings()["cf_account_id"]
        self.chf_mcc = Account.objects.create(id=chf_account_id, name="CHF MCC")

    @generic_test_method()
    def test_update_segment_no_owner_(self, segment_class):
        test_now = datetime(2018, 1, 1, tzinfo=timezone("Etc/GMT+5"))
        segment = segment_class.objects.create()
        with patch_sdb(), patch_now(test_now):
            call_command("update_segments")
        segment.refresh_from_db()
        aw_data = segment.adw_data
        self.assertIsNotNone(aw_data)
        self.assertEqual(aw_data["stats"], EMPTY_STATS)
        self.assertEqual(aw_data["meta"], dict(account_id=str(self.chf_mcc.id),
                                               account_name=self.chf_mcc.name,
                                               updated_at=str(test_now),
                                               is_chf=True))

    @generic_test_method()
    def test_update_segment_no_account_selected_(self, segment_class):
        test_now = datetime(2018, 1, 1, tzinfo=timezone("Etc/GMT+5"))
        user = UserProfile.objects.create(id=1, historical_aw_account=None)
        segment = segment_class.objects.create(owner=user)
        with patch_sdb(), patch_now(test_now):
            call_command("update_segments")
        segment.refresh_from_db()
        aw_data = segment.adw_data
        self.assertIsNotNone(aw_data)
        self.assertEqual(aw_data["stats"], EMPTY_STATS)
        self.assertEqual(aw_data["meta"], dict(account_id=str(self.chf_mcc.id),
                                               account_name=self.chf_mcc.name,
                                               updated_at=str(test_now),
                                               is_chf=True))

    @generic_test_method()
    def test_update_segment_account_selected_(self, segment_class):
        user = UserProfile.objects.create(id=1)
        mcc_account = Account.objects.create(id="manager", name="Some MCC")
        aw_connection = AWConnection.objects.create()
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=aw_connection)
        connection = AWConnectionToUserRelation.objects.create(
            connection=aw_connection, user=user)
        user.historical_aw_account = connection
        user.save()
        test_now = datetime(2018, 1, 1, tzinfo=timezone("Etc/GMT+5"))
        segment = segment_class.objects.create(owner=user)
        with patch_sdb(), patch_now(test_now):
            call_command("update_segments")
        segment.refresh_from_db()
        aw_data = segment.adw_data
        self.assertIsNotNone(aw_data)
        self.assertEqual(aw_data["stats"], EMPTY_STATS)
        self.assertEqual(aw_data["meta"], dict(account_id=mcc_account.id,
                                               account_name=mcc_account.name,
                                               updated_at=str(test_now),
                                               is_chf=False))

    @generic_test_method()
    def test_update_segment_statistic_(self, segment_class):
        user = UserProfile.objects.create(id=1)
        segment = segment_class.objects.create(id=1, owner=user)
        related_id = "123"
        statistic_class = segment_class.related_aw_statistics_model
        related_class = related_classes[segment_class]
        related_class.objects.create(segment=segment,
                                     related_id=related_id)

        mcc_account = Account.objects.create(id="manager", name="Some MCC")
        aw_connection = AWConnection.objects.create()
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=aw_connection)
        connection = AWConnectionToUserRelation.objects.create(
            connection=aw_connection, user=user)
        user.historical_aw_account = connection
        user.save()

        account = Account.objects.create(id="1")
        account.managers.add(mcc_account)
        account.save()
        campaign = Campaign.objects.create(id=1, account=account)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign,
                                          video_views=1)
        clicks, impressions, views, cost = 23, 34, 45, 56
        related_field = get_related_ref(segment_class)
        statistic_class.objects.create(ad_group=ad_group,
                                       date=date(2018, 1, 1),
                                       impressions=impressions,
                                       video_views=views,
                                       clicks=clicks, cost=cost,
                                       **{related_field: related_id})
        expected_stats = dict(
            average_cpv=cost / views,
            ctr=clicks / impressions * 100,
            ctr_v=clicks / views * 100,
            video_view_rate=views / impressions * 100
        )

        test_now = datetime(2018, 1, 1)
        with patch_sdb(), patch_now(test_now):
            call_command("update_segments")
        segment.refresh_from_db()
        aw_data = segment.adw_data
        self.assertIsNotNone(aw_data)
        stats = aw_data["stats"]
        self.assertEqual(stats, expected_stats)
        self.assertEqual(aw_data["meta"], dict(account_id=mcc_account.id,
                                               account_name=mcc_account.name,
                                               updated_at=str(test_now),
                                               is_chf=False))

    @generic_test_method()
    def test_update_aggregate_only_selected_account_(self, segment_class):
        user = UserProfile.objects.create(id=1)
        segment = segment_class.objects.create(id=1, owner=user)
        related_id = "123"
        related_class = related_classes[segment_class]
        related_class.objects.create(segment=segment,
                                     related_id=related_id)
        statistic_class = segment_class.related_aw_statistics_model
        clicks, impressions, cost = 23, 34, 45

        def create_data(uid, views):
            uid = str(uid)
            mcc_account = Account.objects.create(id="mcc_" + uid,
                                                 name="Some MCC " + uid)
            aw_connection = AWConnection.objects.create(email=uid)
            AWAccountPermission.objects.create(account=mcc_account,
                                               aw_connection=aw_connection)
            user_connection = AWConnectionToUserRelation.objects.create(
                connection=aw_connection, user=user)

            account = Account.objects.create(id=uid)
            account.managers.add(mcc_account)
            account.save()

            campaign = Campaign.objects.create(id=uid, account=account)
            ad_group = AdGroup.objects.create(id=uid, campaign=campaign,
                                              video_views=1)
            related_field = get_related_ref(segment_class)
            statistic_class.objects.create(ad_group=ad_group,
                                           date=date(2018, 1, 1),
                                           impressions=impressions,
                                           video_views=views,
                                           clicks=clicks, cost=cost,
                                           **{related_field: related_id})
            return user_connection, mcc_account

        views_1, views_2 = 67, 78
        create_data(1, views_1)
        connection, mcc = create_data(2, views_2)

        user.historical_aw_account = connection
        user.save()

        expected_stats = dict(
            average_cpv=cost / views_2,
            ctr=clicks / impressions * 100,
            ctr_v=clicks / views_2 * 100,
            video_view_rate=views_2 / impressions * 100
        )

        test_now = datetime(2018, 1, 1)
        with patch_sdb(), patch_now(test_now):
            call_command("update_segments")
        segment.refresh_from_db()
        aw_data = segment.adw_data
        self.assertIsNotNone(aw_data)
        stats = aw_data["stats"]
        self.assertEqual(stats, expected_stats)
        self.assertEqual(aw_data["meta"], dict(account_id=mcc.id,
                                               account_name=mcc.name,
                                               updated_at=str(test_now),
                                               is_chf=False))
