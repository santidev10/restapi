import csv
import os
import tempfile

from django.conf import settings

from administration.notifications import send_html_email
from ads_analyzer.reports.account_targeting_report.constants import EXPORT_FIELDS
from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_reporting.models import Account
from saas import celery_app
from .s3_exporter import AccountTargetingReportS3Exporter

EXPORT_FIELDS_SET = set(EXPORT_FIELDS)


@celery_app.task(max_retries=10, retry_backoff=True)
def account_targeting_export(options):
    account = Account.objects.get(id=options["account_id"])
    criteria = options["criteria"]
    aggregation_columns = options["aggregation_columns"]
    aggregation_filters = options.get("aggregation_filters", {})
    statistics_filters = options.get("statistics_filters", {})
    report = AccountTargetingReport(account, criteria)
    report.prepare_report(
        statistics_filters=statistics_filters,
        aggregation_filters=aggregation_filters,
        aggregation_columns=aggregation_columns
    )
    targeting_data = report.get_targeting_report(sort_key="campaign_id")
    # Create separate set for EXPORT_FIELDS to maintain csv order
    export_rows = [
        {
            key: value for key, value in item.items() if key in EXPORT_FIELDS_SET
        }
        for item in targeting_data
    ]
    s3_exporter = AccountTargetingReportS3Exporter()
    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-32", delete=False, suffix=".csv",
                                     dir=settings.TEMPDIR) as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=EXPORT_FIELDS)
        csv_writer.writeheader()
        csv_writer.writerows(export_rows)
    with open(csv_file.name, mode="rb") as file:
        key = s3_exporter.get_s3_key(account.name)
        s3_exporter.export_object_to_s3(file, key)
    os.remove(file.name)
    download_url = s3_exporter.generate_temporary_url(key)
    text_header = "Your Account Targeting Report for: {} is ready".format(account.name)
    text_content = "<a href={download_url}>Click here to download</a>".format(download_url=download_url)
    send_html_email(
        subject=f"Account Targeting Report: {account.name}",
        to=options["recipient"],
        text_header=text_header,
        text_content=text_content,
        from_email=settings.EXPORTS_EMAIL_ADDRESS
    )
