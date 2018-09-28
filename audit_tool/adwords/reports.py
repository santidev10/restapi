import csv
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from typing import Iterator

from audit_tool.dmo import AccountDMO
from utils.utils import safe_exception

from .base import AdwordsBase

logger = logging.getLogger(__name__)


class AdwordsReports(AdwordsBase):
    date = None

    REPORT_FIELDS = (
        "Url",
        "Date",
        "Impressions",
        "CampaignName",
        "AdGroupName",
    )

    def __init__(self, *args, **kwargs):
        self.date = kwargs.pop("date")
        assert type(self.date) is str

        download = kwargs.pop("download", False)

        super().__init__(*args, **kwargs)

        logger.info("Date: {}".format(self.date))

        if download:
            self.download()

    def download(self) -> None:
        self.download_reports()

    def download_reports(self) -> None:
        logger.info("Downloading reports")

        @safe_exception(logger)
        def worker(dmo: AccountDMO) -> None:
            self._download_url_performance_report(dmo)
            downloaded = len([1 for a in self.accounts\
                              if a.url_performance_report is not None])
            if downloaded % 50 == 0:
                logger.info("  {} / {}".format(downloaded, len(self.accounts)))

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = []
            for account in self.accounts:
                futures.append(executor.submit(worker, account))

        reports_count = len(
            [1 for _ in self.accounts if _.url_performance_report is not None]
        )
        logger.info("Downloaded {} report(s)".format(reports_count))

    def get_url_performance_reports(self) -> Iterator[list]:
        for account in self.accounts:
            if account.url_performance_report:
                yield account.url_performance_report

    def get_url_performance_reports_items(self) -> Iterator[list]:
        for report in self.get_url_performance_reports():
            yield from report

    def get_video_reports(self) -> Dict[str, list]:
        videos = {}
        for item in self.get_url_performance_reports_items():
            video_id = item.get("Url", "").split("/")[-1]
            if not video_id:
                continue
            if video_id not in videos:
                videos[video_id] = []
            videos[video_id].append(item)
        return videos

    def _download_url_performance_report(self, dmo: AccountDMO) -> None:
        report_definition = {
            "reportName": "URL_PERFORMANCE_REPORT",
            "dateRangeType": "CUSTOM_DATE",
            "reportType": "URL_PERFORMANCE_REPORT",
            "downloadFormat": "CSV",
            "selector": {
                "predicates": [],
                "fields": self.REPORT_FIELDS,
                "dateRange": {
                    "min": self.date,
                    "max": self.date,
                },
            },
        }

        downloader = dmo.client.GetReportDownloader(version=self.API_VERSION)
        stream = downloader.DownloadReportAsStream(
            report_definition=report_definition,
            skip_report_header=True,
            skip_column_header=True,
            skip_report_summary=True,
            include_zero_impressions=False,
        )
        stream_data = stream.read().decode("utf-8")
        string_io = io.StringIO(stream_data)
        reader = csv.DictReader(string_io, fieldnames=self.REPORT_FIELDS)
        dmo.url_performance_report = []
        for row in reader:
            row["AccountId"] = dmo.account_id
            dmo.url_performance_report.append(row)
