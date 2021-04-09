import random
from datetime import timedelta
from unittest.mock import MagicMock
from unittest.mock import patch

from django.test import TestCase
from django.test import TransactionTestCase
from django.utils import timezone

from aw_reporting.google_ads.tasks.update_pricing_tool_ad_group_stats import DATE_FORMAT
from aw_reporting.google_ads.tasks.update_pricing_tool_ad_group_stats import PricingToolAccountAdGroupStatsUpdater
from aw_reporting.google_ads.utils import AD_WORDS_STABILITY_STATS_DAYS_COUNT
from aw_reporting.models.ad_words.account import Account
from aw_reporting.models.ad_words.ad_group import AdGroup
from aw_reporting.models.ad_words.ad_words import GeoTarget
from aw_reporting.models.ad_words.campaign import Campaign
from aw_reporting.models.ad_words.constants import AdGroupTypeEnum
from aw_reporting.models.ad_words.constants import Device
from aw_reporting.models.statistic import AdGroupGeoViewStatistic
from utils.unittests.int_iterator import int_iterator


class Row:
    pass


class Section:
    pass


class RowFactory:
    """
    factory for creating dummy rows to simulate a google ads report response
    """
    def __init__(self, ad_groups_count=5, geo_targets_count=10, days_diff=365):
        """
        create adgroups and all dependent records so we have a pool of adgroups/geotarget ids to generate rows from
        :param ad_groups_count:
        :param geo_targets_count:
        """
        # create adgroup dependencies
        timezone = "America/Los_Angeles"
        manager_id = next(int_iterator)
        self.manager = Account.objects.create(id=manager_id, name=f"manager_{manager_id}", is_active=True,
                                              timezone=timezone)
        account_id = next(int_iterator)
        self.account = Account.objects.create(id=account_id, name=f"account_{account_id}", is_active=True,
                                              timezone=timezone)
        campaign_id = next(int_iterator)
        self.campaign = Campaign.objects.create(id=campaign_id, name=f"campaign_{campaign_id}", account_id=account_id)
        # create adgroups
        self.ad_groups = list(AdGroup.objects.all()[:ad_groups_count])
        remaining_ad_groups_create_count = ad_groups_count - len(self.ad_groups)
        if remaining_ad_groups_create_count:
            ad_group_types = [e.value for e in AdGroupTypeEnum]
            for _ in range(remaining_ad_groups_create_count):
                ad_group_id = next(int_iterator)
                ad_group = AdGroup(id=ad_group_id, name=f"name_{ad_group_id}", status="enabled",
                                   type=random.choice(ad_group_types), campaign=self.campaign)
                self.ad_groups.append(ad_group)
            AdGroup.objects.bulk_create(self.ad_groups)

        # create geo targets
        geo_target_types = ["Country", "Region", "DMA Region"]
        self.geo_targets = list(GeoTarget.objects.all()[:geo_targets_count])
        remaining_geo_target_create_count = geo_targets_count -len(self.geo_targets)
        if remaining_geo_target_create_count:
            for _ in range(remaining_geo_target_create_count):
                geo_target_id = next(int_iterator)
                geo_target = GeoTarget(id=geo_target_id, name=f"name_{geo_target_id}",
                                       canonical_name=f"can_name_{geo_target_id}", country_code="US",
                                       target_type=random.choice(geo_target_types), status="UNSPECIFIED")
                self.geo_targets.append(geo_target)
            GeoTarget.objects.bulk_create(self.geo_targets)

        # list/pool of day diffs for ensuring unique dates are generated
        self.days_diff_pool = list(range(days_diff))

    def get_account(self):
        """
        get the Account record
        :return:
        """
        return self.account

    def _get_ad_group_id(self):
        """
        get an adgroup id from adgroup pool
        :return:
        """
        ad_group = random.choice(self.ad_groups)
        return ad_group.id

    def _get_date(self):
        """
        get a random date
        :return:
        """
        random.shuffle(self.days_diff_pool)
        days_diff = self.days_diff_pool.pop()
        date = timezone.now() - timedelta(days=days_diff)
        return date.strftime(DATE_FORMAT)

    @staticmethod
    def _get_type_id():
        """
        get a random type id
        :return:
        """
        return random.randrange(6)

    def _get_geo_target_id(self, as_string=False):
        """
        get a random geo target id from geo_target pool, optionally return as_string in the report format
        :param as_string:
        :return:
        """
        geo_target = random.choice(self.geo_targets)
        if as_string:
            return f"geoTargetConstants/{geo_target.id}"
        return geo_target.id

    def _get_geo_target_id_as_string(self):
        """
        alias for _get_geo_target_id where as_string is set to true
        :return:
        """
        return self._get_geo_target_id(as_string=True)

    @staticmethod
    def _get_int():
        """
        get a random integer for stats
        :return:
        """
        return random.randrange(100)

    @staticmethod
    def _get_float():
        """
        get a random float for stats
        :return:
        """
        return random.uniform(0, 10)

    def make(self):
        row = Row()
        for section_name, getter_by_field_name in {
            "ad_group": {
                "id": self._get_ad_group_id,
            },
            "segments": {
                "date": self._get_date,
                "device": self._get_type_id,
                "geo_target_region": self._get_geo_target_id_as_string,
                "geo_target_metro": self._get_geo_target_id_as_string,
            },
            "geographic_view": {
                "country_criterion_id": self._get_geo_target_id,
            },
            "metrics": {
                "impressions": self._get_int,
                "video_views": self._get_int,
                "clicks": self._get_int,
                "cost_micros": self._get_int,
                "conversions": self._get_float,
                "all_conversions": self._get_float,
            },
        }.items():
            section = Section()
            for field_name, func in getter_by_field_name.items():
                setattr(section, field_name, func())

            setattr(row, section_name, section)
        return row


class PricingToolAdGroupStatsUpdaterTestCase(TransactionTestCase):
    """
    uses TransactionTestCase, since TestCase is run inside a transaction. safe_bulk_create, which is used by the
    updater, doesn't like this.
    """

    def test_create_new_records(self):
        """
        test that new stats records are created correctly
        :return:
        """
        existing_stats_count = AdGroupGeoViewStatistic.objects.count()
        client_mock = MagicMock()
        service_mock = client_mock.get_service()
        row_factory = RowFactory()
        row_count = random.randrange(10, 25)
        service_mock.search.return_value = [row_factory.make() for _ in range(row_count)]
        with patch("aw_reporting.google_ads.tasks.update_pricing_tool_ad_group_stats.get_client",
                   return_value=client_mock):

            updater = PricingToolAccountAdGroupStatsUpdater(account=row_factory.get_account())
            updater.run()

            new_stats_count = AdGroupGeoViewStatistic.objects.count()
            self.assertEqual(new_stats_count - existing_stats_count, row_count)

    def test_stats_dropped_only_once(self):
        """
        ensure that stats are only dropped before the first create call. We're lowering the create threshold here to
        trigger the conditional drop multiple times. If stats are dropped more than once, the stats count will be off
        :return:
        """
        existing_stats_count = AdGroupGeoViewStatistic.objects.count()
        client_mock = MagicMock()
        service_mock = client_mock.get_service()
        row_count = 20
        row_factory = RowFactory(days_diff=row_count)
        service_mock.search.return_value = [row_factory.make() for _ in range(row_count)]
        create_threshold = 3
        with patch("aw_reporting.google_ads.tasks.update_pricing_tool_ad_group_stats.get_client",
                   return_value=client_mock),\
                patch("aw_reporting.google_ads.tasks.update_pricing_tool_ad_group_stats.CREATE_THRESHOLD",
                      create_threshold):

            updater = PricingToolAccountAdGroupStatsUpdater(account=row_factory.get_account())
            updater.run()

            new_stats_count = AdGroupGeoViewStatistic.objects.count()
            self.assertEqual(new_stats_count - existing_stats_count, row_count)

    def _get_stats_instances(self, days_diffs: list, row_factory: RowFactory) -> list:
        """
        create AdGroupGeoViewStatistic instances, given a list of day diffs, and a row factory
        :param days_diffs:
        :param row_factory:
        :return:
        """
        stats = []
        for _ in range(len(days_diffs)):
            days_diff = days_diffs.pop()
            timestamp = timezone.now() - timedelta(days=days_diff)
            stat = AdGroupGeoViewStatistic(date=timestamp.date(), ad_group_id=row_factory._get_ad_group_id(),
                                           device_id=Device.MOBILE,
                                           country_id=row_factory._get_geo_target_id(),
                                           region_id=row_factory._get_geo_target_id(),
                                           metro_id=row_factory._get_geo_target_id())
            stats.append(stat)
        return stats

    def test_stats_date_range_deletion(self):
        """
        test that stats outside of the deletion range remain, while those within are deleted, and new stats are added
        :return:
        """
        client_mock = MagicMock()
        service_mock = client_mock.get_service()
        row_count = 20
        row_factory = RowFactory(days_diff=row_count)
        service_mock.search.return_value = [row_factory.make() for _ in range(row_count)]

        # create records that will persist, because they're outside the deletion range
        days_diffs = list(range(AD_WORDS_STABILITY_STATS_DAYS_COUNT + 1,
                                AD_WORDS_STABILITY_STATS_DAYS_COUNT + random.randrange(2, 10)))
        will_persist = self._get_stats_instances(days_diffs=days_diffs, row_factory=row_factory)
        will_persist = AdGroupGeoViewStatistic.objects.bulk_create(will_persist)

        # create reocrds that won't persist, because they're within the deletion range
        days_diffs = list(range(0, AD_WORDS_STABILITY_STATS_DAYS_COUNT - random.randrange(8)))
        wont_persist = self._get_stats_instances(days_diffs=days_diffs, row_factory=row_factory)
        wont_persist = AdGroupGeoViewStatistic.objects.bulk_create(wont_persist)

        existing_stats_count = AdGroupGeoViewStatistic.objects.count()
        with patch("aw_reporting.google_ads.tasks.update_pricing_tool_ad_group_stats.get_client",
                   return_value=client_mock):
            updater = PricingToolAccountAdGroupStatsUpdater(account=row_factory.get_account())
            updater.run()

            new_stats_count = AdGroupGeoViewStatistic.objects.count()
            expected_stats_count = existing_stats_count - len(wont_persist) + row_count

            all_ids = list(AdGroupGeoViewStatistic.objects.values_list("id", flat=True))
            # ensure the records we expect to be dropped are dropped
            self.assertEqual(expected_stats_count, new_stats_count)
            for record in wont_persist:
                with self.subTest(record):
                    self.assertNotIn(record.id, all_ids)
            # ensure records outside the delete range are not dropped
            for record in will_persist:
                with self.subTest(record):
                    self.assertIn(record.id, all_ids)

