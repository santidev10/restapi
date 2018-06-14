from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import timedelta
from googleads import adwords, oauth2
import io
import logging

from oauth2client.client import HttpAccessTokenRefreshError
from suds import WebFault
import yaml

from aw_reporting.models import Account
from aw_reporting.models import AWConnection
from utils.utils import safe_exception
from utils.datetime import now_in_default_tz

from .dmo import AccountDMO


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
    client_options = None
    date_min = None
    date_max = None
    account_ids = None

    API_VERSION = "v201806"
    REPORT_FIELDS = (
        "Url",
        "Date",
        "Impressions",
    )

    def __init__(self, date_min=None, date_max=None, account_ids=None,
                 download=False):
        if date_min is None:
            yesterday = (now_in_default_tz() - timedelta(days=1)).date()
            date_min = yesterday.strftime("%Y%m%d")
        self.date_min = date_min

        if date_max is None:
            date_max = date_min
        self.date_max = date_max

        self.account_ids = account_ids

        if download:
            self.download()

    def download(self):
        logger.info("Dates range: {}..{}".format(self.date_min, self.date_max))
        self.load_client_options()
        self.load_accounts()
        self.resolve_clients()
        self.download_reports()

    def load_client_options(self) -> None:
        with open('aw_reporting/ad_words_web.yaml', 'r') as f:
            self.client_options = yaml.load(f)

    def load_accounts(self) -> None:
        logger.info("Loading accounts")
        self.accounts = []

        # load managers
        managers = {}
        queryset = Account.objects.filter(can_manage_clients=False)\
                                  .values("id", "managers")
        if self.account_ids is not None:
            queryset = queryset.filter(id__in=self.account_ids)

        for account in queryset:
            id = account["id"]
            if id not in managers:
                managers[id] = []
            managers[id].append(account["managers"])

        # load connections
        refresh_tokens = {}
        queryset = AWConnection.objects.filter(
            mcc_permissions__can_read=True,
            revoked_access=False,
        ).values("mcc_permissions__account", "refresh_token")
        for connection in queryset:
            id = connection["mcc_permissions__account"]
            refresh_tokens[id] = connection["refresh_token"]

        # collect accounts
        for account_id in sorted(managers.keys()):
            tokens = [refresh_tokens[i] for i in managers[account_id]]
            self.accounts.append(
                AccountDMO(
                    client_customer_id=account_id,
                    refresh_tokens=tokens,
                )
            )

        logger.info("Loaded {} account(s)".format(len(self.accounts)))

    def resolve_clients(self) -> None:
        assert self.accounts is not None
        logger.info("Resolving clients")

        @safe_exception(logger)
        def worker(dmo: AccountDMO) -> None:
            self._resolve_client(dmo)

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for _ in self.accounts:
                executor.submit(worker, _)

        logger.info("Resolved {} client(s)".format(len(
            [1 for a in self.accounts if a.client is not None])
        ))

    def download_reports(self) -> None:
        assert self.accounts is not None
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
            for i, account in enumerate(self.accounts):
                futures.append(executor.submit(worker, account))

        logger.info("Downloaded {} report(s)".format(len(
            [1 for a in self.accounts if a.url_performance_report is not None])
        ))

    def get_url_performance_reports(self):
        assert self.accounts is not None
        for account in self.accounts:
            if account.url_performance_report:
                yield account.url_performance_report

    def get_url_performance_reports_items(self):
        for report in self.get_url_performance_reports():
            yield from report

    def get_videos(self):
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
                    dmo.client_customer_id,
                    refresh_token,
                )
            except (HttpAccessTokenRefreshError, WebFault):
                continue
            else:
                return
        raise Exception("No valid refresh tokens found")

    def _get_client_by_token(self, client_customer_id: str,
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
            client_customer_id=client_customer_id,
            enable_compression=True,
        )

        return client

    def _download_url_performance_report(self, dmo: AccountDMO) -> None:
        assert self.date_min is not None
        assert self.date_max is not None

        report_definition = {
            "reportName": "URL_PERFORMANCE_REPORT",
            "dateRangeType": "CUSTOM_DATE",
            "reportType": "URL_PERFORMANCE_REPORT",
            "downloadFormat": "CSV",
            "selector": {
                "predicates": [],
                "fields": self.REPORT_FIELDS,
                "dateRange": {
                    "min": self.date_min,
                    "max": self.date_max,
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
        string_io = io.StringIO(stream.read().decode('utf-8'))
        reader = csv.DictReader(string_io, fieldnames=self.REPORT_FIELDS)
        dmo.url_performance_report = []
        for row in reader:
            row['AccountId'] = dmo.client_customer_id
            dmo.url_performance_report.append(row)
