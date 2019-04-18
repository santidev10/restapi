import pytz
from datetime import datetime
from datetime import time
from datetime import timedelta

from django.conf import settings
from django.test import TestCase


from aw_reporting.csv_reports import PacingReportCSVExport
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.reports.pacing_report import PacingReport
from utils.aws.s3 import get_s3_client
from utils.utittests.s3_mock import mock_s3


S3_BUCKET = settings.AMAZON_S3_REPORTS_BUCKET_NAME


class PacingReportCSVExportTestCase(TestCase):
    @mock_s3
    def test_success(self):
        report_name = "PacingReport-test"

        today = datetime.now()
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        start = today - timedelta(days=3)
        end = today - timedelta(days=2)

        opportunity = Opportunity.objects.create(
            id='1', name="", start=start, end=end, probability=100
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        Flight.objects.create(
            id="1", name="", placement=placement, start=start, end=end, total_cost=10, ordered_units=100,
        )
        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(
            id="1", account=account, name="", salesforce_placement=placement, video_views=102,
        )
        CampaignStatistic.objects.create(date=start, campaign=campaign, video_views=102)

        pacing_report = PacingReport()
        opportunities = pacing_report.get_opportunities({})

        csv_generator = PacingReportCSVExport(pacing_report, opportunities, report_name)
        csv_generator.export_to_s3()

        expected_key = "pacing-reports/{report_name}.csv".format(
            report_name=report_name,
        )
        s3 = get_s3_client()
        s3_objects = s3.list_objects(Bucket=S3_BUCKET)["Contents"]
        self.assertEqual(len(s3_objects), 1)
        self.assertEqual(s3_objects[0]["Key"], expected_key)