from datetime import date
from unittest import mock
from unittest import skipIf

from django.test import TransactionTestCase

from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from segment.models import SegmentChannel
from segment.models import SegmentKeyword
from segment.models import SegmentRelatedChannel
from segment.models import SegmentRelatedKeyword
from segment.models import SegmentRelatedVideo
from segment.models import SegmentVideo
from segment.models import SegmentRelatedKeywordManager
from segment.models import SegmentRelatedVideoManager
from segment.models import SegmentRelatedChannelManager
from singledb.connector import SingleDatabaseApiConnector
from userprofile.models import UserProfile
from utils.utittests.generic_test import generic_test
from utils.utittests.redis_mock import MockRedis


related_classes = {
    SegmentChannel: SegmentRelatedChannel,
    SegmentVideo: SegmentRelatedVideo,
    SegmentKeyword: SegmentRelatedKeyword,
}


def get_related_ref(segment_class):
    return "keyword" \
        if segment_class is SegmentKeyword \
        else "yt_id"


@skipIf(True, "v1 Custom Segments deprecated")
class UpdateSegmentsTestCase(TransactionTestCase):
    generic_args_list = [
        ("Channel Segment", (SegmentChannel, SegmentRelatedChannelManager), dict()),
        ("Video Segment", (SegmentVideo, SegmentRelatedVideoManager), dict()),
        ("Keyword Segment", (SegmentKeyword, SegmentRelatedKeywordManager), dict()),
    ]

    def setUp(self):
        chf_account_id = load_web_app_settings()["cf_account_id"]
        self.chf_mcc = Account.objects.create(id=chf_account_id, name="CHF MCC")
        self.chf_mcc.refresh_from_db()

        self.redis_mock = mock.patch("utils.celery.tasks.REDIS_CLIENT", MockRedis())
        self.redis_mock.start()

    def tearDown(self):
        self.redis_mock.stop()

    @generic_test(generic_args_list)
    def test_cleanup(self, segment_class, segment_related_manager):
        user = UserProfile.objects.create(id=1)
        segment = segment_class.objects.create(id=1, owner=user)

        alive_related_ids = ["1", "2", "3", "4", "5"]
        deleted_related_ids = ["20", "21"]
        related_class = related_classes[segment_class]
        for related_id in alive_related_ids + deleted_related_ids:
            related_class.objects.create(segment=segment,
                                         related_id=related_id)

        statistic_class = segment_class.related_aw_statistics_model

        mcc_account = self.chf_mcc
        aw_connection = AWConnection.objects.create()
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=aw_connection)

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

        with mock.patch.object(segment_related_manager, "_get_alive_singledb_data", return_value=alive_related_ids):
            related_class.objects.cleanup_related_records()

        self.assertEqual(list(segment.get_related_ids()), alive_related_ids)

    @generic_test(generic_args_list)
    def test_cleanup_huge_segments(self, segment_class, segment_related_manager):

        deleted_related_ids = [str(id) for id in range(10000)]
        alive_related_ids = [str(id) for id in range(10000, 10009)]

        def mocked_connector_store_ids(*args, **kwargs):
            return args[0]

        def mocked_connector_get_data(*args, **kwargs):
            alive_ids = set(args[0]).intersection(set(alive_related_ids))
            return alive_ids

        user = UserProfile.objects.create(id=1)
        segment = segment_class.objects.create(id=1, owner=user)

        related_class = related_classes[segment_class]
        for related_id in alive_related_ids + deleted_related_ids:
            related_class.objects.create(segment=segment,
                                         related_id=related_id)

        assert len(segment.get_related_ids()) == 10009

        statistic_class = segment_class.related_aw_statistics_model

        mcc_account = self.chf_mcc
        aw_connection = AWConnection.objects.create()
        AWAccountPermission.objects.create(account=mcc_account,
                                           aw_connection=aw_connection)

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

        with mock.patch.object(SingleDatabaseApiConnector, "store_ids", side_effect=mocked_connector_store_ids):
            with mock.patch.object(segment_related_manager, "_get_alive_singledb_data", side_effect=mocked_connector_get_data):
                related_class.objects.cleanup_related_records()

        self.assertEqual(list(segment.get_related_ids()), alive_related_ids)