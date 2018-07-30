from concurrent.futures import ThreadPoolExecutor
import csv
from googleads import adwords, oauth2
import io
import logging
from oauth2client.client import HttpAccessTokenRefreshError
from suds import WebFault
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple
import yaml

from audit_tool.dmo import AccountDMO
from utils.utils import safe_exception


logger = logging.getLogger(__name__)

logging.getLogger("oauth2client.client")\
    .setLevel(logging.WARNING)

logging.getLogger("googleads.adwords.report_downloader")\
    .setLevel(logging.WARNING)

logging.getLogger("requests")\
    .setLevel(logging.WARNING)


class AdWords:
    MAX_WORKERS = 50

    accounts = None
    date_start = None
    date_finish = None

    client_options = None

    API_VERSION = "v201806"
    REPORT_FIELDS = (
        "Url",
        "Date",
        "Impressions",
        "CampaignName",
        "AdGroupName",
    )

    def __init__(self,
                 accounts: List[AccountDMO],
                 date_start: str,
                 date_finish: str,
                 download: bool = False,
                 save_filename: str = None,
                 load_filename: str = None,
                 fields: Tuple[str] = None,
                 ):

        self.accounts = accounts
        self.date_start = date_start
        self.date_finish = date_finish
        self.save_filename = save_filename
        self.load_filename = load_filename
        if fields:
            self.REPORT_FIELDS = fields

        logger.info("Dates range: {}..{}".format(
            self.date_start,
            self.date_finish
        ))

        if load_filename:
            self.load_reports()
        elif download:
            self.download()

    def download(self) -> None:
        self.load_client_options()
        self.resolve_clients()
        self.download_reports()
        if self.save_filename is not None:
            self.save_reports()

    def load_client_options(self) -> None:
        with open('aw_reporting/ad_words_web.yaml', 'r') as f:
            self.client_options = yaml.load(f)

    def resolve_clients(self) -> None:
        assert self.accounts is not None
        logger.info("Resolving clients")

        @safe_exception(logger)
        def worker(dmo: AccountDMO) -> None:
            self._resolve_client(dmo)

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for _ in self.accounts:
                executor.submit(worker, _)

        clients_count = len([1 for a in self.accounts if a.client is not None])
        logger.info("Resolved {} client(s)".format(clients_count))

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

    def save_reports(self) -> None:
        assert self.save_filename is not None
        logger.info("Saving reports to '{}'".format(self.save_filename))
        with open(self.save_filename, "w") as f:
            writer = csv.DictWriter(f, fieldnames=["ID", "AccountId"] + list(self.REPORT_FIELDS))
            writer.writeheader()
            for video_id, report_rows in self.get_video_reports().items():
                for row in report_rows:
                    row["ID"] = video_id
                    writer.writerow(row)
        logger.info("All reports have been saved")

    def load_reports(self) -> None:
        assert self.load_filename is not None
        logger.info("Loading reports from '{}'".format(self.load_filename))
        with open(self.load_filename, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                del row["ID"]
                dmo = None
                for account in self.accounts:
                    if account.account_id == row.get("AccountId"):
                        dmo = account
                        break
                if dmo is None:
                    continue
                if dmo.url_performance_report is None:
                    dmo.url_performance_report = []
                dmo.url_performance_report.append(row)
        logger.info("All reports have been loaded")

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

    def _resolve_client(self, dmo: AccountDMO) -> None:
        for refresh_token in dmo.refresh_tokens:
            try:
                dmo.client = self._get_client_by_token(
                    dmo.account_id,
                    refresh_token,
                )
            except (HttpAccessTokenRefreshError, WebFault):
                continue
            else:
                return
        raise Exception("No valid refresh tokens found")

    def _get_client_by_token(self,
                             account_id: str,
                             refresh_token: str) -> adwords.AdWordsClient:
        assert self.client_options is not None

        oauth2_client = oauth2.GoogleRefreshTokenClient(
            self.client_options["client_id"],
            self.client_options["client_secret"],
            refresh_token,
        )

        client = adwords.AdWordsClient(
            self.client_options["developer_token"],
            oauth2_client,
            user_agent=self.client_options["user_agent"],
            client_customer_id=account_id,
            enable_compression=True,
        )

        return client

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
                    "min": self.date_start,
                    "max": self.date_finish,
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
