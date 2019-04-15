import csv
import os
import tempfile

from django.conf import settings

from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from utils.aws.s3_exporter import S3Exporter
from utils.serializers.fields import PercentField

OPPORTUNITY_COLUMN_NAME = "opportunity_name"
PLACEMENT_COLUMN_NAME = "placement_name"
FLIGHT_COLUMN_NAME = "flight_name"
CAMPAIGN_COLUMN_NAME = "campaign_name"

S3_PACING_REPORT_EXPORT_KEY_PATTERN = "pacing-reports/{report_name}.csv"
REPORT_NAME = "PacingReport"


class SharedColumn:
    PACING = "pacing"
    MARGIN = "margin"
    IO = "io_start"
    START = "start"
    END = "end"
    PLAN_COST = "plan_cost"
    PLAN_VIDEO_VIEWS = "plan_video_views"
    PLAN_CPV = "plan_cpv"
    PLAN_IMPRESSIONS = "plan_impressions"
    PLAN_CPM = "plan_cpm"
    COST = "cost"
    VIDEO_VIEWS = "video_views"
    CPV = "cpv"
    IMPRESSIONS = "impressions"
    CPM = "cpm"


class OpportunityColumn(SharedColumn):
    AD_OPS = "ad_ops"
    AM = "am"
    SALES = "sales"
    CATEGORY = "category"
    REGION = "region"


CSV_COLUMN_ORDER = (
    OPPORTUNITY_COLUMN_NAME,
    PLACEMENT_COLUMN_NAME,
    FLIGHT_COLUMN_NAME,
    CAMPAIGN_COLUMN_NAME,
    SharedColumn.PACING,
    SharedColumn.MARGIN,
    SharedColumn.IO,
    SharedColumn.START,
    SharedColumn.END,
    SharedColumn.PLAN_COST,
    SharedColumn.PLAN_VIDEO_VIEWS,
    SharedColumn.PLAN_CPV,
    SharedColumn.PLAN_IMPRESSIONS,
    SharedColumn.PLAN_CPM,
    SharedColumn.COST,
    SharedColumn.VIDEO_VIEWS,
    SharedColumn.CPV,
    SharedColumn.IMPRESSIONS,
    SharedColumn.CPM,
    OpportunityColumn.AD_OPS,
    OpportunityColumn.AM,
    OpportunityColumn.SALES,
    OpportunityColumn.CATEGORY,
    OpportunityColumn.REGION,
)


REPORT_HEADERS = {
    OPPORTUNITY_COLUMN_NAME: "Name.Opportunity",
    PLACEMENT_COLUMN_NAME: "Name.Placement",
    FLIGHT_COLUMN_NAME: "Name.Flight",
    CAMPAIGN_COLUMN_NAME: "Name.Campaign",
    SharedColumn.PACING: "KPIs.Pacing",
    SharedColumn.MARGIN: "KPIs.Margin",
    SharedColumn.IO: "Dates.IO",
    SharedColumn.START: "Dates.Start",
    SharedColumn.END: "Dates.End",
    SharedColumn.PLAN_COST: "Goals.Budget",
    SharedColumn.PLAN_VIDEO_VIEWS: "Goals.Views",
    SharedColumn.PLAN_CPV: "Goals.CPV",
    SharedColumn.PLAN_IMPRESSIONS: "Goals.Impressions",
    SharedColumn.PLAN_CPM: "Goals.CPM",
    SharedColumn.COST: "Delivered.Cost",
    SharedColumn.VIDEO_VIEWS: "Delivered.Views",
    SharedColumn.CPV: "Delivered.CPV",
    SharedColumn.IMPRESSIONS: "Delivered.Impressions",
    SharedColumn.CPM: "Delivered.CPM",
    OpportunityColumn.AD_OPS: "AdOps",
    OpportunityColumn.AM: "AM",
    OpportunityColumn.SALES: "Sales",
    OpportunityColumn.CATEGORY: "Category",
    OpportunityColumn.REGION: "Territory",
}

FORMATS = {
    "money": "${:0,.2f}",
    "percent": "{0:.2f}%",
    "number": "{:,.0f}",
    "date": "{:%Y-%m-%d}",
    "default": "{}"
}

FORMATTING = dict(
    pacing=FORMATS["percent"],
    margin=FORMATS["percent"],
    io_start=FORMATS["date"],
    start=FORMATS["date"],
    end=FORMATS["date"],
    plan_cost=FORMATS["money"],
    plan_cpv=FORMATS["money"],
    plan_cpm=FORMATS["money"],
    cost=FORMATS["money"],
    cpv=FORMATS["money"],
    cpm=FORMATS["money"],
    plan_video_views=FORMATS["number"],
    video_views=FORMATS["number"],
    plan_impressions=FORMATS["number"],
    impressions=FORMATS["number"]
)


class PacingReportCSVExport:

    def __init__(self, report, opportunities, report_name=None):
        self.data_generator = self.pacing_report_list(report, opportunities)
        self.report_name = report_name

    def _map_row(self, row):
        result_list = []
        for column in CSV_COLUMN_ORDER:
            value = row.get(column)
            if value:

                if isinstance(value, float) and (column == SharedColumn.PACING or column == SharedColumn.MARGIN):
                    value = PercentField().to_representation(value)

                column_format = FORMATTING.get(column, FORMATS["default"])
                try:
                    value = column_format.format(value)
                except:
                    # except error with formatting headers (Name.Opportunity, ...)
                    pass

            result_list.append(value)

        return result_list

    def export_generator(self):
        """
        Export data generator
        """
        yield self._map_row(REPORT_HEADERS)
        for row in self.data_generator:
            yield self._map_row(row)

    def pacing_report_list(self, report, opportunities):

        for opportunity in opportunities:
            # opportunity
            opportunity[OPPORTUNITY_COLUMN_NAME] = opportunity.get("name")

            for column in (OpportunityColumn.AD_OPS, OpportunityColumn.AM,
                           OpportunityColumn.SALES, OpportunityColumn.CATEGORY, OpportunityColumn.REGION):
                value = opportunity.get(column)
                if value:
                    opportunity[column] = value.get("name")

            yield opportunity

            # placements
            opportunity_object = Opportunity.objects.get(id=opportunity["id"])
            for placement in report.get_placements(opportunity_object):

                placement[PLACEMENT_COLUMN_NAME] = placement.get("name")

                yield placement

                # flights
                placement_obj = OpPlacement.objects.get(id=placement["id"])
                for flight in report.get_flights(placement_obj):

                    flight[FLIGHT_COLUMN_NAME] = flight.get("name")

                    yield flight

                    # campaigns
                    flight_obj = Flight.objects.get(id=flight["id"])
                    for campaign in report.get_campaigns(flight_obj):

                        campaign[CAMPAIGN_COLUMN_NAME] = campaign.get("name")

                        yield campaign

    def export_to_s3(self):
        content_exporter = PacingReportExportContent(self.export_generator)
        PacingReportS3Exporter.export_to_s3(content_exporter, self.report_name)


class PacingReportS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = "application/CSV"

    @staticmethod
    def get_s3_key(name):
        key = S3_PACING_REPORT_EXPORT_KEY_PATTERN.format(report_name=name)
        return key


class PacingReportExportContent:

    def __init__(self, export_generator):
        self.export_generator = export_generator

    def __enter__(self):
        _, self.filename = tempfile.mkstemp(dir=settings.TEMPDIR)

        with open(self.filename, mode="w+", newline="") as export_file:
            writer = csv.writer(export_file)
            for row in self.export_generator():
                writer.writerow(row)
        return self.filename

    def __exit__(self, *args):
        os.remove(self.filename)
